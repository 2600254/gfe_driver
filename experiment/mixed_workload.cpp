//
// Created by per on 29.09.21.
//

#include "mixed_workload.hpp"

#include <future>
#include <chrono>
#include <fstream>


#if defined(HAVE_OPENMP)
  #include "omp.h"
#endif

#include "graphalytics.hpp"
#include "aging2_experiment.hpp"
#include "mixed_workload_result.hpp"

namespace gfe::experiment {

    using namespace std;

    MixedWorkloadResult MixedWorkload::execute() {
      std::ofstream throughput("Throughput.txt");
      bool done=false;
      chrono::seconds progress_check_interval( 1 );
      auto aging_result_future = std::async(std::launch::async, &Aging2Experiment::execute, &m_aging_experiment);
      auto start_time = chrono::steady_clock::now();
      auto x=std::async(std::launch::async,[&](){
        uint64_t last=0;
        while(!done)
        {
          auto now=m_aging_experiment.num_operations_sofar();
          throughput<<now-last<<" "<<std::chrono::duration_cast<std::chrono::milliseconds>(chrono::steady_clock::now()-start_time).count()<<std::endl;
          this_thread::sleep_for( progress_check_interval ) ;
        }
        return true;
      });
      this_thread::sleep_for( progress_check_interval );  // Poor mans synchronization to ensure AgingExperiment was able to setup the master etc
      while (true) {
        if (m_aging_experiment.progress_so_far() > 0.1 || aging_result_future.wait_for(std::chrono::seconds(0)) == std::future_status::ready) { // The graph reached its final size
          break;
        }
        this_thread::sleep_for( progress_check_interval ) ;
      }
      cout << "Graph reached final size, executing graphaltyics now" << endl;

      // TODO change this to also work for LCC, generally this solution is very ugly
#if defined(HAVE_OPENMP)
      if(m_read_threads != 0 ){
                        cout << "[driver] OpenMP, number of threads for the Graphalytics suite: " << m_read_threads << endl;
                        omp_set_num_threads(m_read_threads);
                    }
#endif

      while (m_aging_experiment.progress_so_far() < 0.9 && aging_result_future.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
        m_graphalytics.execute(start_time,true);
      }

      cout << "Waiting for aging experiment to finish" << endl;
      aging_result_future.wait();
      cout << "Getting aging experiment results" << endl;
      auto aging_result = aging_result_future.get();

      return MixedWorkloadResult { aging_result, m_graphalytics };
    }

}