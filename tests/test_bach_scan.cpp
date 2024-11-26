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
#include <tbb/concurrent_hash_map.h>

#include "common/system.hpp"
#include "configuration.hpp"
#include "graph/edge.hpp"
#include "graph/edge_stream.hpp"
#include "library/baseline/csr.hpp"
#include "library/bach/bach_driver.hpp"
#include "third-party/bach/BACH.h"
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

TEST(BACH, SCAN)
{
    //auto stream = make_shared<gfe::graph::WeightedEdgeStream>("/home/b2600254/data/wiki-Talk/wiki-Talk.properties");
    auto stream = make_shared<gfe::graph::WeightedEdgeStream> ("/home/b2600254/data/datagen-7_5-fb/datagen-7_5-fb.properties" );
    BACHDriver bach{/* directed ? */ false};

    LOG("Read graph ...");
    stream->permute();

    LOG("Insert " << stream->num_edges() << " edges into BACH ...")
    uint64_t sz = stream->num_edges();
    unordered_map<uint64_t, uint64_t> answer;
    unordered_map<uint64_t, uint64_t> result;
    // #pragma omp parallel for
    for (uint64_t i = 0; i < sz; i++)
    {
        auto e = stream->get(i);
        if (answer.find(e.source()) != answer.end())
            ++answer[e.source()]; // 修改值
        else
            answer.insert({e.source(), 1});
        if (answer.find(e.destination()) != answer.end())
            ++answer[e.destination()]; // 修改值
        else
            answer.insert({e.destination(), 1});
        bach.add_edge_v2(e);
    }
    LOG("Insert done")
    int x;
    std::cin >> x;
    auto txn = reinterpret_cast<bach::DB *>(bach.bach())->BeginReadOnlyTransaction();
    txn.EdgeLabelScan(0, [&result, &bach](bach::vertex_t src, bach::vertex_t dst, bach::edge_property_t property) -> void
                      {
        if (result.find(src)!=result.end())
            ++result[src]; // 修改值
        else
            result.insert({src,1}); });
    LOG("Scan done")
    for (auto &i : answer)
    {
        auto j = result.find(bach.ext2int(i.first));
        if (j->second != i.second)
        {
            std::cout << j->first << " " << j->second << " " << i.first << " " << i.second;
        }
        ASSERT_TRUE(j->second == i.second);
    }
}

#else
// #include <iostream>
TEST(BACH, Disabled)
{
    std::cout << "Tests disabled as the build does not contain the support for BACH.\n";
}
#endif
