/**
  @Author: Iswarya Hariharan
  Wave2D simulation with MPI and openmp
**/
#include <iostream>
#include "Timer.h"
#include <stdlib.h>            // atoi
#include <cmath>
#include <cstring>
#include "mpi.h"               //MPI
#include <omp.h>               // OpenMP

int default_size = 100;        // the default system size
int defaultCellWidth = 8;
double c_wavespeed = 1.0;      // wave speed
double dt = 0.1;               // time quantum
double dd = 2.0;               // change in system

using namespace std;

int main( int argc, char *argv[] ) {
  // verify arguments
  if ( argc != 5 ) {
    cerr << "usage: Wave2D size max_time interval threads" << endl;
    return -1;
  }
  int size = atoi( argv[1] );
  int max_time = atoi( argv[2] );
  int interval  = atoi( argv[3] );
  int n_threads = atoi( argv[4] );
  double time_quantum = (0.1/2.0);
  double time_quantum_sq = pow(time_quantum, 2);
  double wave_height = pow(c_wavespeed, 2);
  double (*a)[size][size], (*b)[size][size], (*c)[size][size];
  double (*curr)[size], (*prev)[size], (*next)[size];
  if ( size < 100 || max_time < 3 || interval < 0 ) {
    cerr << "usage: Wave2D size max_time interval" << endl;
    cerr << "       where size >= 100 && time >= 3 && interval >= 0" << endl;
    return -1;
  }
  int my_rank = 0;                            // used by MPI
  int mpi_size = 1;                           // used by MPI 
  bool print_option = false;                  // print out c[] if it is true
  int tag = 0;                                //Tag for messages
  Timer time;                                 //Rank 0 - uses the timer
  MPI_Init( &argc, &argv );                   // start MPI
  MPI_Comm_rank( MPI_COMM_WORLD, &my_rank );  // Get the rank of each process
  MPI_Comm_size( MPI_COMM_WORLD, &mpi_size ); //Get number of processes: ranks
  MPI_Status status;                          //Stores status od messages received
  /* create a simulation space in all the ranks */
  double z[3][size][size];
  #pragma omp parallel for num_threads(n_threads)
    for ( int p = 0; p < 3; p++ ) 
      for ( int i = 0; i < size; i++ )
        for ( int j = 0; j < size; j++ )
  	       z[p][i][j] = 0.0;                  // no wave
  if(my_rank == 0) {
    time.start( );                            //Rank 0 - start a timer
  }
  /* At time = 0; initialize the simulation space: calculate z[0][][] */
  int weight = size / default_size;
  #pragma omp parallel for num_threads(n_threads)
  for( int i = 0; i < size; i++ ) {
    for( int j = 0; j < size; j++ ) {
      if( i > 40 * weight && i < 60 * weight  &&
	  j > 40 * weight && j < 60 * weight ) {
	z[0][i][j] = 20.0;
      } else {
	z[0][i][j] = 0.0;
      }
    }
  }
  /* When time = 1, calculate z[1][][] */
  #pragma omp parallel for num_threads(n_threads)
  for( int i = 0; i < size; i++ ) {
    for( int j = 0; j < size; j++ ) {
      if( i == 0 || i == (size-1) || j == 0 || j == (size-1))
        z[1][i][j] = 0.0;
      else
        z[1][i][j] = z[0][i][j] + (wave_height / 2.0) * time_quantum_sq * (z[0][i+1][j] + z[0][i-1][j] + z[0][i][j-1] + z[0][i][j+1] - 4.0 * z[0][i][j]);
    }
  }
  int curr_index = 2, prev_index_1 = 1, prev_index_2 = 0;   //Indices for accessing z[] using rotation
  int stripe = size / mpi_size;                             // partitioned stripe
  int my_stripe_limit = my_rank*stripe + stripe;            //Stripe limit for each rank
  /* Simulate wave from t=2 */
  for(int t = 2; t < max_time; t++) {
    /* Start passing data from rank 0 to all ranks*/
    if(my_rank == 0) {
      for(int rank = 1; rank < mpi_size; rank++) {
        MPI_Send(&z[prev_index_2][stripe * rank][0], stripe * size, MPI_DOUBLE, rank, tag, MPI_COMM_WORLD);
        MPI_Send(&z[prev_index_1][stripe * rank][0], stripe * size, MPI_DOUBLE, rank, tag, MPI_COMM_WORLD);
        if(rank == 1) {
          /* Pass boundary data to rank 1 */
          MPI_Send(&z[prev_index_1][(stripe * rank) - 1][0], size, MPI_DOUBLE, rank, tag, MPI_COMM_WORLD);
        }
      }      
    }
    else {
      for(int i = 0; i < 2; i++) {
        MPI_Recv(&z[i][stripe * my_rank][0], stripe * size, MPI_DOUBLE, 0, tag, MPI_COMM_WORLD, &status);
      }
      if(my_rank % 2 == 1) {  
        /* Odd ranks: First receive boundary data from neighbouring ranks and then pass to the neighbours */ 
        MPI_Recv(&z[1][(stripe * my_rank) - 1][0], size, MPI_DOUBLE, my_rank-1, tag, MPI_COMM_WORLD, &status);
        if(my_rank != (mpi_size - 1)) {
          MPI_Recv(&z[1][(stripe * (my_rank + 1))][0], size, MPI_DOUBLE, my_rank+1, tag, MPI_COMM_WORLD, &status);
        }
        if(my_rank - 1 != 0)
          MPI_Send(&z[1][stripe * my_rank][0], size, MPI_DOUBLE, my_rank-1, tag, MPI_COMM_WORLD);
        if(my_rank + 1 <= mpi_size-1)
          MPI_Send(&z[1][stripe * (my_rank+1) - 1 ][0], size, MPI_DOUBLE, my_rank+1, tag, MPI_COMM_WORLD);
      }
      else {  
      /* Even numbered ranks : First send boundary data to neighbours and then receive from them */
          MPI_Send(&z[1][stripe * my_rank][0], size, MPI_DOUBLE, my_rank-1, tag, MPI_COMM_WORLD);
          MPI_Send(&z[1][stripe * (my_rank+1) - 1 ][0], size, MPI_DOUBLE, my_rank+1, tag, MPI_COMM_WORLD);
          MPI_Recv(&z[1][(stripe * my_rank) - 1][0], size, MPI_DOUBLE, my_rank-1, tag, MPI_COMM_WORLD, &status);
          MPI_Recv(&z[1][stripe * (my_rank + 1)][0], size, MPI_DOUBLE, my_rank+1, tag, MPI_COMM_WORLD, &status);
      }
    }
    /*All ranks compute the z[t] for its stripe*/
    #pragma omp parallel for schedule(dynamic) num_threads(n_threads)
    for( int i = my_rank*stripe; i < my_stripe_limit; i++ ) {
      for( int j = 0; j < size; j++ ) {
        if( i == 0 || i == (size-1) || j == 0 || j == (size-1))          
          z[curr_index][i][j] = 0.0;
        else {
          z[curr_index][i][j] = 2.0 * z[prev_index_1][i][j] - z[prev_index_2][i][j] + wave_height * time_quantum_sq * (z[prev_index_1][(i+1)][j] + z[prev_index_1][(i-1)][j] + z[prev_index_1][i][(j+1)] + z[prev_index_1][i][(j-1)] - 4 * z[prev_index_1][i][j]);
        }          
      }
    }
    /* Send computed stripe values to rank 0, if rank=0: receive all computed values from other ranks */
    if(my_rank != 0) {
      MPI_Send(&z[2][stripe * my_rank][0], stripe * size, MPI_DOUBLE, 0, tag, MPI_COMM_WORLD);
    }
    else {
      for(int rank = 1; rank < mpi_size; rank++) {
        MPI_Recv(&z[curr_index][(stripe * rank)][0], stripe * size, MPI_DOUBLE, rank, tag, MPI_COMM_WORLD, &status);
      }
      /* print values of z[t] according to the interval*/
      if( interval > 0 && t % interval == 0 ) {
        cout<<t<<"\n";
        for( int j = 0; j < size; j++ ) {
          for( int i = 0; i < size; i++ ) {
            cout<<z[curr_index][i][j]<<" ";
          }
          cout<<"\n";
        }
      }
      /* Update indices to access z[t] in next iteration*/
      curr_index = (curr_index + 1) % 3;
      prev_index_1 = (prev_index_1 + 1) % 3;
      prev_index_2 = (prev_index_2 + 1) % 3;
    }
  }  
  cerr<<"rank["<<my_rank<<"] range = "<<my_rank*stripe<<" ~ "<<(my_rank*stripe + stripe)-1<<endl; // print range of values each rank handles
  if(my_rank == 0) {
    cerr << "Elapsed time = " << time.lap( ) << endl;                                             // Rank :0 finish the timer
  }
  MPI_Finalize( );                                                                                 //Shutdown MPI
  return 0;
}
