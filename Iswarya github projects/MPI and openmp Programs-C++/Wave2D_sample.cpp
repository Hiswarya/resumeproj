#include <iostream>
#include "Timer.h"
#include <stdlib.h>            // atoi
#include <cmath>

int default_size = 100;        // the default system size
int defaultCellWidth = 8;
double c_wavespeed = 1.0;      // wave speed
double dt = 0.1;               // time quantum
double dd = 2.0;               // change in system

using namespace std;

int main( int argc, char *argv[] ) {
  // verify arguments
  if ( argc != 4 ) {
    cerr << "usage: Wave2D size max_time interval" << endl;
    return -1;
  }
  int size = atoi( argv[1] );
  int max_time = atoi( argv[2] );
  int interval  = atoi( argv[3] );
  double time_quantum = (dt/dd);
  double time_quantum_sq = pow(time_quantum, 2);
  double wave_height = pow(c_wavespeed, 2);

  if ( size < 100 || max_time < 3 || interval < 0 ) {
    cerr << "usage: Wave2D size max_time interval" << endl;
    cerr << "       where size >= 100 && time >= 3 && interval >= 0" << endl;
    return -1;
  }
  // create a simulation space
  double z[3][size][size];
  for ( int p = 0; p < 3; p++ ) 
    for ( int i = 0; i < size; i++ )
      for ( int j = 0; j < size; j++ )
	z[p][i][j] = 0.0; // no wave

  // start a timer
  Timer time;
  time.start( );
  // time = 0;
  // initialize the simulation space: calculate z[0][][]
  int weight = size / default_size;
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
  // For t=1 calculate z[1][][]
  // for t==1 , schrodinger formula computation Zt_i,j = Zt-1_i,j + c2 / 2 * (dt/dd)2 * (Zt-1_i+1,j + Zt-1_i-1,j + Zt-1_i,j+1 + Zt-1_i,j-1
  // – 4.0 * Zt-1_i,j)
  for( int i = 0; i < size; i++ ) {
    for( int j = 0; j < size; j++ ) {
      if( i == 0 || i == (size-1) || j == 0 || j == (size-1))
        z[1][i][j] = 0.0;
      else
        z[1][i][j] = z[0][i][j] + (wave_height / 2.0) * time_quantum_sq * (z[0][i+1][j] + z[0][i-1][j] + z[0][i][j-1] + z[0][i][j+1] - 4.0 * z[0][i][j]);
    }
  }
  // simulate wave diffusion from time = 2
  int curr_index = 2, prev_index_1 = 1, prev_index_2 = 0;
  for(int t = 2; t < max_time; t++) {
    for( int i = 0; i < size; i++ ) {
      for( int j = 0; j < size; j++ ) {
        if( i == 0 || i == (size-1) || j == 0 || j == (size-1))          
          z[curr_index][i][j] = 0.0;
        else
          z[curr_index][i][j] = 2.0 * z[prev_index_1][i][j] - z[prev_index_2][i][j] + wave_height * time_quantum_sq * (z[prev_index_1][(i+1)][j] + z[prev_index_1][(i-1)][j] + z[prev_index_1][i][(j+1)] + z[prev_index_1][i][(j-1)] - 4 * z[prev_index_1][i][j]);
      }
    }
    if( t % interval == 0 ) {
      cout<<t<<"\n";
      for( int j = 0; j < size; j++ ) {
        for( int i = 0; i < size; i++ ) {
          cout<<z[curr_index][j][i]<<" ";
        }
        cout<<"\n";
      }
    }
    curr_index = (curr_index + 1) % 3;
    prev_index_1 = (prev_index_1 + 1) % 3;
    prev_index_2 = (prev_index_2 + 1) % 3;
  }    
  // finish the timer
  cerr << "Elapsed time = " << time.lap( ) << endl;
  return 0;
}
