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
#include "gtest/gtest.h"
#if defined(HAVE_BACH)

#include <cstdlib>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "common/system.hpp"
#include "configuration.hpp"
#include "graph/edge.hpp"
#include "graph/edge_stream.hpp"
#include "library/baseline/csr.hpp"
#include "library/bach/bach_driver.hpp"
#include "utility/graphalytics_validate.hpp"

// Log to stdout
#undef LOG
#define LOG(message)                                      \
    {                                                     \
        std::cout << "\033[0;32m"                         \
                  << "[          ] "                      \
                  << "\033[0;0m" << message << std::endl; \
    }

using namespace common::concurrency;
using namespace gfe::graph;
using namespace gfe::library;
using namespace std;

static std::unique_ptr<gfe::graph::WeightedEdgeStream> generate_edge_stream(uint64_t max_vector_id = 8)
{
    vector<gfe::graph::WeightedEdge> edges;
    for (uint64_t i = 1; i < max_vector_id; i++)
    {
        for (uint64_t j = i + 2; j < max_vector_id; j += 2)
        {
            edges.push_back(gfe::graph::WeightedEdge{i, j, static_cast<double>(j * 1000 + i)});
        }
    }
    return make_unique<gfe::graph::WeightedEdgeStream>(edges);
}

// Get the path to non existing temporary file
static string temp_file_path()
{
    char pattern[] = "/tmp/gfe_XXXXXX";
    int fd = mkstemp(pattern);
    if (fd < 0)
    {
        ERROR("Cannot obtain a temporary file");
    }
    close(fd); // we're going to overwrite this file anyway
    return string(pattern);
}

// TEST(BACH, Updates)
// {
//     BACHDriver bach{false};
//     uint64_t num_vertices = 1ull << 4;
//     LOG("Creating a graph ...");
//     auto stream = generate_edge_stream(num_vertices);
//     stream->permute();
//     LOG("Insert " << stream->num_edges() << " edges into LiveGraph ...")
//     uint64_t sz = stream->num_edges();
//     for (uint64_t i = 0; i < sz; i++)
//     {
//         LOG(stream->get(i));
//         bach.add_edge_v2(stream->get(i));
//     }
// }

TEST(BACH, BFSInternalImpl)
{
    CSR csr{/* directed ? */ false};
    BACHDriver bach{/* directed ? */ false};

    uint64_t num_vertices = 1ull << 13;

    LOG("Creating a graph ...");
    auto stream = generate_edge_stream(num_vertices);
    stream->permute();

    LOG("Insert " << stream->num_edges() << " edges into BACH ...")
    uint64_t sz = stream->num_edges();
#pragma omp parallel for
    for (uint64_t i = 0; i < sz; i++)
    {
        bach.add_edge_v2(stream->get(i));
    }

    LOG("Load the stream into the CSR ...");
    csr.load(*(stream.get()));
    stream.reset();

    uint64_t num_iterations = 1;
    LOG("BFS number of iterations: " << num_iterations);

    auto csr_results = temp_file_path();
    LOG("CSR BFS: " << csr_results << " ...");
    csr.bfs(num_iterations, csr_results.c_str());

    auto bach_results = temp_file_path();
    LOG("BACH BFS: " << bach_results << " ...");
    bach.bfs(num_iterations, bach_results.c_str());

    LOG("Validate the result ...");
    gfe::utility::GraphalyticsValidate::bfs(bach_results, csr_results);
    LOG("Validation succeeded");
}

TEST(BACH, PageRankInternalImpl)
{
    CSR csr{/* directed ? */ false};
    BACHDriver bach{/* directed ? */ false};

    uint64_t num_vertices = 1ull << 13;

    LOG("Creating a graph ...");
    auto stream = generate_edge_stream(num_vertices);
    stream->permute();

    LOG("Insert " << stream->num_edges() << " edges into BACH ...")
    uint64_t sz = stream->num_edges();
#pragma omp parallel for
    for (uint64_t i = 0; i < sz; i++)
    {
        bach.add_edge_v2(stream->get(i));
    }

    LOG("Load the stream into the CSR ...");
    csr.load(*(stream.get()));
    stream.reset();

    uint64_t num_iterations = 1;
    LOG("PageRank number of iterations: " << num_iterations);

    auto csr_results = temp_file_path();
    LOG("CSR PageRank: " << csr_results << " ...");
    csr.pagerank(num_iterations, 0.85, csr_results.c_str());

    auto bach_results = temp_file_path();
    LOG("BACH PageRank: " << bach_results << " ...");
    bach.pagerank(num_iterations, 0.85, bach_results.c_str());

    LOG("Validate the result ...");
    gfe::utility::GraphalyticsValidate::pagerank(bach_results, csr_results);
    LOG("Validation succeeded");
}

#else
#include <iostream>
TEST(BACH, Disabled)
{
    std::cout << "Tests disabled as the build does not contain the support for BACH.\n";
}
#endif
