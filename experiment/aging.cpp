/**
 * Copyright (C) 2019 Dean De Leo, email: dleo[at]cwi.nl
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include "aging.hpp"

#include <cassert>
#include <random>
#include <thread>
#include <vector>

#include <gmpxx.h> // libgmp

#include "common/database.hpp"
#include "common/optimisation.hpp"
#include "common/system.hpp"
#include "common/timer.hpp"
#include "details/async_batch.hpp"
#include "graph/edge.hpp"
#include "graph/edge_stream.hpp"
#include "library/interface.hpp"
#include "configuration.hpp"

using namespace common;
using namespace std;

namespace experiment {

/*****************************************************************************
 *                                                                           *
 * Debug                                                                     *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { LOG("[Aging::" << __FUNCTION__ << "] [" << concurrency::get_thread_id() << "] " << msg); }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif


Aging::Aging(std::shared_ptr<library::UpdateInterface> interface, std::shared_ptr<graph::WeightedEdgeStream> stream, double mult_num_operations, int64_t num_threads) :
        Aging(interface,stream, stream->num_edges() * mult_num_operations, num_threads, interface->is_directed(), stream->max_weight()) { }

Aging::Aging(std::shared_ptr<library::UpdateInterface> interface, std::shared_ptr<graph::WeightedEdgeStream> stream, uint64_t num_operations, int64_t num_threads, bool graph_is_directed, double max_weight) :
        m_interface(interface), m_stream(stream),
        m_vertices_final(move(* (stream->vertex_table().get()) ) ),
        m_num_operations_total(num_operations), m_num_threads(num_threads), m_num_edges(stream->num_edges()),
        m_max_vertex_id(std::max<uint64_t>(/* avoid overflows */ stream->max_vertex_id(), stream->max_vertex_id() * /* noise */ 2)),
        m_is_directed(graph_is_directed), m_max_weight(max_weight){
    LOG("Aging experiment initialised. Number of threads: " << m_num_threads << ", number of operations: " << m_num_operations_total);

#if defined(DEBUG)
    set_report_progress(true);
#endif
}

void Aging::set_expansion_factor(double factor){
    if(factor < 1) INVALID_ARGUMENT("the expansion factor must be >= 1, instead the value given is: " << factor);
    LOG("[Aging] Expansion factor set to: " << factor);
    m_expansion_factor = factor;
}

void Aging::set_operation_granularity(uint64_t granularity){
    if(granularity < 1) INVALID_ARGUMENT("the granularity given must be > 0: " << granularity);
    LOG("[Aging] Granularity set to: " << granularity);
    m_granularity = granularity;
}

void Aging::set_batch_size(uint64_t size){
    m_batch_size = size;
}

void Aging::set_report_progress(bool value){
    m_report_progress = value;
}

vector<vector<Aging::Partition>> Aging::compute_partitions_directed() const{
    vector<vector<Partition>> partitions ( m_num_threads );
    int64_t num_partitions = m_num_threads * 4;
    int64_t part_length = (m_max_vertex_id +1) / num_partitions;
    int64_t odd_partitions = (m_max_vertex_id +1) % num_partitions;
    int64_t start = 0;

    for(int64_t part_id = 0; part_id < num_partitions; part_id++){
        int64_t length = part_length + (part_id < odd_partitions);
        partitions[part_id % m_num_threads].emplace_back(start, length);
        start += length; // next partition
    }
    return partitions;
}

vector<vector<Aging::Partition>> Aging::compute_partitions_undirected() const{
    vector<vector<Partition>> partitions ( m_num_threads );
    int64_t num_partitions = m_num_threads * 8;
    mpz_class max_num_edges = mpz_class{m_max_vertex_id} * mpz_class{m_max_vertex_id} / 2; // (n-1)*(n-2) /2;
    mpz_class e = max_num_edges / num_partitions; // edges per partition

    LOG("Computing the list of partitions...");
    COUT_DEBUG("num_partitions: " << num_partitions << ", max_vertex_id: " << m_max_vertex_id << ", max_num_edges: " << max_num_edges << ", edges_per_partition: " << e);

    uint64_t vertex_from = 0;
    int64_t part_id = 0;
    mpz_t tmp_x; mpz_init(tmp_x); // temporary
    constexpr size_t buffer_sz = 1024; char buffer[buffer_sz];

    while(part_id < num_partitions -1 && vertex_from < (m_max_vertex_id -1)){
        // how many edges can we create from vertex_from ?
        mpz_class S = mpz_class{m_max_vertex_id +1 -vertex_from} * mpz_class{m_max_vertex_id -vertex_from} /2;

        // solve the inequation S - [(x^2 +x) /2] < e
        // that is x^2 +x +2(e -s) > 0 => [ -1 + sqrt( 1 - 8(e-s) ) ] / 2
//        uint64_t x = ceil( (-1.0 + sqrt(1ll - 8ll*(e-S)) ) / 2.0 );
        mpz_class sqrt_arg = ( S - e ) * 8 +1;
        mpz_sqrt(tmp_x, sqrt_arg.get_mpz_t());
        mpz_sub_ui(tmp_x, tmp_x, 1u);
        mpz_cdiv_q_ui(tmp_x, tmp_x, 2u); // compute the quotient & round up (the `c' in cdiv)

        // convert the result back to uint64_t
        assert(mpz_sizeinbase(tmp_x, 10) <= buffer_sz && "Conversion overflow");
        mpz_get_str(buffer, 10, tmp_x);
        uint64_t x = strtoull(buffer, nullptr, 10);

        assert(x <= m_max_vertex_id);
        uint64_t vertex_upto = m_max_vertex_id - x;
        if(vertex_from == vertex_upto) vertex_upto++; // corner case
        else if(vertex_upto >= m_max_vertex_id) vertex_upto = (m_max_vertex_id -1); // corner case

        COUT_DEBUG("partition[" << part_id << "] interval: [" << vertex_from << ", " << vertex_upto << ")");
        partitions[part_id % m_num_threads].emplace_back(vertex_from, vertex_upto - vertex_from);

        vertex_from = vertex_upto; // next iteration
        part_id++;
    }
    mpz_clear(tmp_x);

    COUT_DEBUG("partition[" << part_id << "] interval: [" << vertex_from << ", " << m_max_vertex_id << "]" );
    partitions[part_id % m_num_threads].emplace_back(vertex_from, m_max_vertex_id - vertex_from +1);

    return partitions;
}


bool Aging::is_directed() const {
    return m_is_directed;
}

void Aging::insert_vertex(uint64_t vertex_id){
    if(m_vertices_present.insert(vertex_id, true)){
        COUT_DEBUG("insert vertex: " << vertex_id);
        m_interface->add_vertex(vertex_id);
    }
}

void Aging::all_workers_execute(const vector<unique_ptr<AgingThread>>& workers, AgingOperation operation){
    vector<future<void>> sync;
    for(auto& w: workers){ sync.push_back( w->execute(operation) ); }
    for(auto& s: sync) { s.get(); }
}

// run the experiment
std::chrono::microseconds Aging::execute(){
    auto interface = m_interface.get();
    int num_workers =  m_num_threads * 2 + /* main */ 1; // * 2 due to batch async
    interface->on_main_init(num_workers);
    interface->on_thread_init(0);

    // compute the set of partitions for each worker
    vector<vector<Partition>> partitions = is_directed() ? compute_partitions_directed() : compute_partitions_undirected();
    assert(partitions.size() == m_num_threads && "Expected one partition set per thread");

    // start the threads
    LOG("[Aging] Initialising " << m_num_threads << " threads ...");
    vector<thread> threads; threads.reserve(m_num_threads);
    vector<unique_ptr<AgingThread>> workers; workers.reserve(m_num_threads);
    for(int i = 0; i < m_num_threads; i++){
        int worker_id = i * 2 +1; // worker_id is for the AgingThread, and worker_id +1 is for the AsyncBatch
        workers.push_back(make_unique<AgingThread>( this, partitions[i], worker_id ));
        threads.emplace_back(&AgingThread::main_thread, workers[i].get());
    }

    // init all threads
    all_workers_execute(workers, AgingOperation::START);
    all_workers_execute(workers, AgingOperation::COMPUTE_FINAL_EDGES);
    m_stream.reset(); // we don't need anymore the list of edges

    // execute the experiment
    LOG("[Aging] Experiment started!");
    m_last_progress_reported = 0;
    Timer timer; timer.start();
    all_workers_execute(workers, AgingOperation::EXECUTE_EXPERIMENT);
    timer.stop();
    LOG("[Aging] Experiment done!");
    LOG("[Aging] Updates performed with " << m_num_threads << " threads in " << timer);
    m_completion_time = timer.microseconds();
    auto completion_time_microsecs = timer.duration<chrono::microseconds>();

    {
        LOG("[Aging] Removing the vertices in excess, that should not appear in the final graph ...");
        timer.start();
        all_workers_execute(workers, AgingOperation::INTERNAL_CLEANUP); // free up some memory
        // remove all vertices that are not present in the final graph
        auto lst_vertices = m_vertices_present.lock_table();
        m_vertices2remove = new uint64_t[lst_vertices.size()];
        uint64_t vertices2remove_sz = 0;
        for(auto vertex : lst_vertices){
            if(!m_vertices_final.contains(vertex.first)){
                m_vertices2remove[vertices2remove_sz] = vertex.first;
                vertices2remove_sz++;
    //            m_interface->remove_vertex(vertex.first);
            }
        }
        lst_vertices.unlock();
        uint64_t items_per_part = vertices2remove_sz / workers.size();
        uint64_t odd_items = vertices2remove_sz % workers.size();
        uint64_t start = 0;
        for(uint64_t i = 0; i < workers.size(); i++){
            uint64_t length = items_per_part + (i < odd_items);
            workers[i]->set_partition_vertices_to_remove(start, length);
            start += length; // next iteration
        }
        all_workers_execute(workers, AgingOperation::REMOVE_VERTICES);
        delete[] m_vertices2remove; m_vertices2remove = nullptr;
        timer.stop();
        LOG("[Aging] Artificial vertices removed in " << timer);
    }


    // stop all threads
    LOG("[Aging] Waiting for all worker threads to stop...");
    all_workers_execute(workers, AgingOperation::STOP);
    for(auto& t : threads) t.join();
    LOG("[Aging] Worker threads terminated");


    interface->on_thread_destroy(0); // main

    return completion_time_microsecs;
}


void Aging::save() {
    assert(configuration().db() != nullptr);
    auto db = configuration().db()->add("aging");
    db.add("granularity", m_granularity);
    db.add("num_threads", m_num_threads);
    db.add("num_updates_requested", m_num_operations_total);
    db.add("num_updates_executed", m_num_operations_performed);
    db.add("completion_time", m_completion_time); // microseconds
}

/*****************************************************************************
 *                                                                           *
 *  Aging thread                                                             *
 *                                                                           *
 *****************************************************************************/
Aging::AgingThread::AgingThread(Aging* instance, const std::vector<Partition>& partitions, int worker_id) : m_instance(instance),
        m_interface(instance->m_interface.get()), m_worker_id(worker_id), m_is_undirected(!m_instance->is_directed()),
        m_partitions(partitions), m_batch(nullptr) {

    if(m_instance->m_batch_size > 0){ // execute updates in batches
        m_batch = new details::AsyncBatch(m_interface, worker_id +1, 4, m_instance->m_batch_size);
    }

    // compute the number of vertices we are responsible to handle
    for(uint64_t part_id = 0, sz = partitions.size(); part_id < sz; part_id++){
        m_num_src_vertices_in_partitions += partitions[part_id].m_length;
    }

    if(is_undirected()){ // compute the max number of edges in each partition
        m_num_edges_in_partition = new mpz_class[m_partitions.size()];
        mpz_class M { m_instance->m_max_vertex_id };
        for(uint64_t part_id = 0, sz = m_partitions.size(); part_id < sz; part_id++){
            mpz_class start { m_partitions[part_id].m_start }; // incl.
            mpz_class end = start + m_partitions[part_id].m_length; // excl.
            mpz_class sum1 = (M - start) * (M - start +1) / 2;
            mpz_class sum2 = (M - end) * (M - end +1) / 2;
            assert(sum1 > sum2);
            m_num_edges_in_partition[part_id] = sum1 - sum2;
        }
    }
}

Aging::AgingThread::~AgingThread(){
    delete m_batch; m_batch = nullptr;
    delete[](m_num_edges_in_partition); m_num_edges_in_partition = nullptr;
}

std::future<void> Aging::AgingThread::execute(AgingOperation operation){
    auto current_op = m_current_operation;
    if(current_op != AgingOperation::NONE) ERROR("Invalid state: " << (int64_t) operation << ". The worker id " << m_worker_id << " is already busy performing another operation");

    // critical section
    unique_lock<mutex> lock(m_mutex_op);
    m_callback = promise<void>{ };
    auto future = m_callback.get_future();
    m_current_operation = operation;
    lock.unlock();

    m_condvar_op.notify_all();

    return future;
}

void Aging::AgingThread::main_thread(){
    while(true){
        unique_lock<mutex> lock(m_mutex_op);
        m_condvar_op.wait(lock, [this](){ return m_current_operation != AgingOperation::NONE; } );

        while(m_current_operation == AgingOperation::NONE) { } // active wait
        if(m_current_operation == AgingOperation::STOP) break; // exit from the loop

        switch(m_current_operation){
        case AgingOperation::START:
            m_interface->on_thread_init(m_worker_id);
            break;
        case AgingOperation::COMPUTE_FINAL_EDGES: {
            m_edges.clear();
            m_final_edges_current_position = 0;
            graph::WeightedEdgeStream* stream = m_instance->m_stream.get();
            for(uint64_t i = 0, end = stream->num_edges(); i < end; i++){
                auto edge = stream->get(i);
                if(is_undirected() && edge.source() > edge.destination()) edge.swap_src_dst(); // ensure src_id < dst_id for undirected graphs
                if(vertex_belongs(edge.source())) m_edges.push_back(edge);
            }
        } break;
        case AgingOperation::EXECUTE_EXPERIMENT: {
            main_experiment();
        } break;
        case AgingOperation::INTERNAL_CLEANUP: {
            m_edges_already_inserted.clear();
        } break;
        case AgingOperation::REMOVE_VERTICES : {
            uint64_t* __restrict vertices = m_instance->m_vertices2remove + m_interval_vertices2remove.m_start;
            for(uint64_t i = 0, sz = m_interval_vertices2remove.m_length; i < sz; i++){
                m_interface->remove_vertex(vertices[i]);
            }
        } break;
        default:
            assert(false && "Invalid operation");
        }

        m_current_operation = AgingOperation::NONE; // move on
        lock.unlock(); // release the lock
        m_callback.set_value();
    }

    assert(m_current_operation == AgingOperation::STOP && "Invalid state, it should still be in the loop");
    m_interface->on_thread_destroy(m_worker_id);
    m_callback.set_value_at_thread_exit();
}

void Aging::AgingThread::main_experiment(){
    // constants
    const uint64_t max_number_edges = static_cast<uint64_t>(m_instance->m_expansion_factor * m_instance->m_num_edges);
    const int64_t num_total_ops = static_cast<int64_t>(m_instance->m_num_operations_total);
    // heuristics to bump up the probability of inserting a final edge due to multiple threads and deletions
    const double prob_bump = 1.0 * m_instance->m_num_threads;
    const bool report_progress = m_instance->m_report_progress;

    mpz_class max_num_edges = !is_undirected() ? /* ignore */ mpz_class{ 0 } : get_num_edges_in_my_partitions();
    uniform_int_distribution<uint64_t> genrndsrc {0, m_num_src_vertices_in_partitions -1 }; // incl.
    uniform_int_distribution<uint64_t> genrnddst {0, m_instance->m_max_vertex_id }; // incl
//    uniform_int_distribution<uint64_t> genrndarc {0, max_edge_id > numeric_limits<uint64_t>::max() ?  numeric_limits<uint64_t>::max() : static_cast<uint64_t>(max_edge_id) }; // incl
    gmp_randclass genrndarc(gmp_randinit_default); genrndarc.seed(std::random_device{}());
    uniform_real_distribution<double> genrndweight{0, m_instance->m_max_weight }; // incl.

    int64_t num_ops_done = 0;
    while( (num_ops_done = m_instance->m_num_operations_performed.fetch_add(m_instance->m_granularity) ) < num_total_ops ){

        // shall we perform a burst of insertions or deletions ?
        if(m_edges2remove.empty() /* There are no edges to remove */ ||
                m_interface->num_edges() < max_number_edges /* the size of the current graph is no more than (exp_factor)x of the final graph */){

            if(report_progress && static_cast<int>(100.0 * num_ops_done/num_total_ops) != m_instance->m_last_progress_reported){
                m_instance->m_last_progress_reported = 100.0 * num_ops_done/num_total_ops;
                LOG("[thread: " << common::concurrency::get_thread_id() << "] "
                       "Progress: " << num_ops_done << "/" << num_total_ops << " (" << 100.0 * num_ops_done/num_total_ops << "%), "
                       "edges processed: " <<  m_final_edges_current_position << " / " << m_edges.size() << " (" << (100.0 * m_final_edges_current_position/m_edges.size()) << " %)"
                );
            }

            // insert `m_granularity' edges then
            for(int64_t i = 0, end = m_instance->m_granularity; i < end; i++){
                double prob_insert_final = prob_bump * static_cast<double>(missing_edges_final()) / (num_total_ops - num_ops_done);
                if ( m_uniform(m_random) < prob_insert_final){ // insert from the final graph
                    assert(m_final_edges_current_position < m_edges.size());

                    auto edge = m_edges[m_final_edges_current_position];
                    m_final_edges_current_position++;
                    assert((!is_undirected() || edge.source() < edge.destination()) && "Edges in undirected graphs should always be retrieved with the src < dst");

                    // if this edge has been previously inserted remove it
                    auto raw_edge = edge.edge();
//                    COUT_DEBUG("[Prob: " << prob_insert_final << "] ADD_EDGE FINAL: " << edge);
                    auto res = m_edges_already_inserted.insert_or_assign(raw_edge, /* final */ true);
                    if(! res.second ){ /* the edge was already present */
                        remove_edge(raw_edge);
                    }

                    insert_edge(edge);

                } else {
                    // insert a random edge (noise)
                    uint64_t src_id {0}, dst_id {0};
                    double weight = genrndweight(m_random);

                    if(is_undirected()) {
                        mpz_class edge_id = genrndarc.get_z_range(max_num_edges); // generate a random value in [0, max_num_edges)
                        edge_id_2_vertices_id(edge_id, &src_id, &dst_id);
                        assert(src_id < dst_id && "for undirected graphs, any edge should be generated with src < dst");
                    } else {
                        src_id = src_rel2abs(genrndsrc(m_random));
                        dst_id = genrnddst(m_random);
                        if(dst_id == src_id){ // avoid having the same src & dst for an edge
                            dst_id = (dst_id != m_instance->m_max_vertex_id) ? dst_id +1 : 0;
                        }
                    }

                    graph::WeightedEdge edge { src_id, dst_id, weight };
                    auto raw_edge = edge.edge();
                    auto res = m_edges_already_inserted.find(raw_edge);
                    bool do_insert = true;
                    bool is_already_registered = false;
                    if(res == m_edges_already_inserted.end()){ // this edge is not already present
                        m_edges_already_inserted[raw_edge] = false;
                    } else if (!res->second){
                        // already present, but it's not final, overwrite its value
                        is_already_registered = true;
                        remove_edge(raw_edge);
                    } else {
                        // already present and final, that is it belongs to the final graph
                        do_insert = false;
                    }

                    if(do_insert){
//                        COUT_DEBUG("[Prob: " << prob_insert_final << "] ADD_EDGE TEMP: " << edge);
                        insert_edge(edge);
                        if(!is_already_registered) m_edges2remove.append(raw_edge); /* otherwise already present in the list of edges to remove */
                    } /* else {
                        global_operation_count--;
                    }*/
                }
            }
        } else {
            // perform a burst of deletions
            for(uint64_t i = 0, end = std::min<uint64_t>(m_instance->m_granularity, m_edges2remove.size()); i < end && !m_edges2remove.empty(); i++){
                remove_temporary_edge();
            }
        } // end if (burst of insertions or deletions)

    } // end while (operation count)

    // insert the missing edges from the final graph
    COUT_DEBUG("Processed edges: " << m_final_edges_current_position << " / " << m_edges.size() << " (" << (100.0 * m_final_edges_current_position/m_edges.size()) << " %)");

    for( ; m_final_edges_current_position < m_edges.size(); m_final_edges_current_position++){
        auto edge = m_edges[m_final_edges_current_position];

        // if this edge has been previously inserted remove it
        auto raw_edge = edge.edge();
        auto res = m_edges_already_inserted.insert_or_assign(raw_edge, /* final */ true);

//        COUT_DEBUG("ADD_EDGE FINAL [2]: " << edge);
        if(! res.second ){ /* the edge was already present */
            remove_edge(raw_edge);
        }

        insert_edge(edge);
    }

    // remove all edges that do not belong to the final graph
    while(!m_edges2remove.empty()){
        remove_temporary_edge();
    }

    // done!
    if(m_batch) m_batch->flush(true); // process all pending updates
}

void Aging::AgingThread::insert_edge(graph::WeightedEdge edge){
//    COUT_DEBUG("edge: " << edge);

    // be sure that the vertices source & destination are already present
    m_instance->insert_vertex(edge.source());
    m_instance->insert_vertex(edge.destination());

    if(is_undirected() && random01() < 0.5) edge.swap_src_dst(); // noise

    if(m_batch == nullptr){
        // the function returns true if the edge has been inserted. Repeat the loop if it cannot insert the edge as one of
        // the vertices is still being inserted by another thread
        while ( ! m_interface->add_edge(edge) ) { /* nop */ };
    } else {
        m_batch->add_edge(edge);
    }
}

void Aging::AgingThread::remove_edge(graph::Edge edge){
//    COUT_DEBUG("edge: " << edge);

    if(is_undirected() && random01() < 0.5) edge.swap_src_dst(); // noise

    if(m_batch == nullptr){
        m_interface->remove_edge(edge);
    } else { // batch updates
        m_batch->remove_edge(edge);
    }
}

double Aging::AgingThread::random01() noexcept {
    return m_uniform(m_random);
}

void Aging::AgingThread::remove_temporary_edge(){
    assert(!m_edges2remove.empty());

    bool removed = false;
    while(!m_edges2remove.empty() && !removed){
        auto edge = m_edges2remove[0]; m_edges2remove.pop();
        assert(m_edges_already_inserted.find(edge) != m_edges_already_inserted.end() && "This should be in the list of the edges inserted");
        auto it = m_edges_already_inserted.find(edge);
        if(it->second == false /* this is not an edge of the final graph */ ){
//            COUT_DEBUG("DELETE_EDGE TEMP: " << edge);
            remove_edge(edge);
            m_edges_already_inserted.erase(it);
            removed = true;
        }
    }
}

int64_t Aging::AgingThread::missing_edges_final() const {
    assert(m_final_edges_current_position <= m_edges.size());
    return static_cast<int64_t>(m_edges.size() - m_final_edges_current_position);
}

mpz_class Aging::AgingThread::get_num_edges_in_my_partitions() const {
    assert(is_undirected() && "Only for undirected graphs");

    mpz_class total;

    for(uint64_t part_id = 0; part_id < m_partitions.size(); part_id++){
        total += m_num_edges_in_partition[part_id];
    }

    return total;
}

void Aging::AgingThread::edge_id_2_vertices_id(const mpz_class& edge_id, uint64_t* out_src_id, uint64_t* out_dst_id){
    assert(is_undirected() && "Only for undirected graphs");
    assert(out_src_id != nullptr && out_dst_id != nullptr && "Output argument missing");
    *out_src_id = *out_dst_id = 0; // reset the initial values
    mpz_t tmp_x; mpz_init(tmp_x); // temporary

    bool stop = false;
    mpz_class e = edge_id;
    uint64_t part_id = 0;
    while(!stop && part_id < m_partitions.size()){
        if(e >= m_num_edges_in_partition[part_id]){
            e -= m_num_edges_in_partition[part_id];
            part_id ++;
        } else {
            stop = true;
        }
    }
    assert(stop == true && "The given edge ID is outside the partitions of this worker");

    const mpz_class M = m_instance->m_max_vertex_id;
    mpz_class v0 = m_partitions[part_id].m_start;
    mpz_class S = (M +1 -v0) * (M -v0) /2;
    // Again we need to solve the inequality S - [(x^2 +x) /2] <= e
    // that is x^2 +x +2(e -s) >= 0 --> [ -1 + sqrt( 1 - 8(e-s) ) ] / 2
//    uint64_t x = ceil( (-1.0 + sqrt(1ll - 8ll*(e-S)) ) / 2.0 );
    mpz_class sqrt_arg = ( S - e ) * 8 +1;
    mpz_sqrt(tmp_x, sqrt_arg.get_mpz_t());
    mpz_sub_ui(tmp_x, tmp_x, 1u);
    mpz_cdiv_q_ui(tmp_x, tmp_x, 2u); // compute the quotient & round up (the `c' in cdiv)
    assert(mpz_class(tmp_x) <= numeric_limits<uint64_t>::max() && "Overflow");

    // convert the result back to uint64_t
    constexpr uint64_t buffer_sz = 256; char buffer[buffer_sz];
    assert(mpz_sizeinbase(tmp_x, 10) <= buffer_sz && "Conversion overflow");
    mpz_get_str(buffer, 10, tmp_x);
    uint64_t x = strtoull(buffer, nullptr, 10);

    // compute the offset from src_id to dst_id
    uint64_t src_id = m_instance->m_max_vertex_id -x;
    /*bool*/ stop = false;
    do { // we repeat the computation if we obtain a negative offset, it means we went too far when selecting `src_id'
        mpz_class S1 = (M+1 - src_id) * (M - src_id) /2;
        assert(S1 <= S);

        mpz_set(tmp_x, S.get_mpz_t()); // S
        mpz_sub(tmp_x, tmp_x, S1.get_mpz_t()); // S - S1
        mpz_sub(tmp_x, e.get_mpz_t(), tmp_x); // e - [ S - S1 ]

        if(mpz_cmp_si(tmp_x, 0) < 0){ // underflow => negative offset
            src_id --;
        } else {
            stop = true; // done
        }
    } while (!stop);
    assert(mpz_sizeinbase(tmp_x, 10) <= buffer_sz && "Conversion overflow");
    mpz_get_str(buffer, 10, tmp_x);
    uint64_t offset = strtoull(buffer, nullptr, 10);

    uint64_t dst_id = src_id + 1 + offset;

    assert(src_id < dst_id);
//    COUT_DEBUG("edge_id: " << edge_id << ", M: " << M << ", v0: " << v0 << ", S: " << S << ", x: " << x << ", src_id: " << src_id << ", S1:"  << S1 << ", dst_id: " << dst_id);
//    COUT_DEBUG("edge_id: " << edge_id << ", src_id: " << src_id << ", dst_id: " << dst_id);
    *out_src_id = src_id;
    *out_dst_id = dst_id;

    mpz_clear(tmp_x);
}

uint64_t Aging::AgingThread::src_rel2abs(uint64_t relative_vertex_id) const {
    assert(! m_partitions.empty() && "There are no partitions for this worker");
    int64_t count = static_cast<int64_t>(relative_vertex_id);
    int64_t i = 0, sz = m_partitions.size();
    while(i < sz){
        int64_t length = m_partitions[i].m_length; // cast to int64_t
        if(count - length < 0){
            return m_partitions[i].m_start + count;
        } else {
            count -= length;
            i++;
        }
    }

    assert(false && "Invalid vertex ID");
    ERROR("Invalid relative vertex ID: " << relative_vertex_id);
}

bool Aging::AgingThread::vertex_belongs(uint64_t vertex_id) const {
    for(auto& p: m_partitions){
        uint64_t start = p.m_start;
        uint64_t end = start + p.m_length;
        if(vertex_id >= start && vertex_id < end){
            return true;
        }
    }

    return false;
}

void Aging::AgingThread::set_partition_vertices_to_remove(uint64_t start, uint64_t length){
    COUT_DEBUG("start: " << start << ", length: " << length);
    m_interval_vertices2remove = { start, length };
}


} // namespace experiment
