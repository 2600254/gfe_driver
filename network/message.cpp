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

#include "message.hpp"

namespace network {

std::ostream& operator<<(std::ostream& out, RequestType type){
    switch(type){
    case RequestType::TERMINATE_SERVER: out << "TERMINATE_SERVER"; break;
    case RequestType::TERMINATE_WORKER: out << "TERMINATE_WORKER"; break;
    case RequestType::ON_MAIN_INIT: out << "ON_MAIN_INIT"; break;
    case RequestType::ON_THREAD_INIT: out << "ON_THREAD_INIT"; break;
    case RequestType::ON_THREAD_DESTROY: out << "ON_THREAD_DESTROY"; break;
    case RequestType::ON_MAIN_DESTROY: out << "ON_MAIN_DESTROY"; break;
    case RequestType::NUM_EDGES: out << "NUM_EDGES"; break;
    case RequestType::NUM_VERTICES: out << "NUM_VERTICES"; break;
    case RequestType::HAS_VERTEX: out << "HAS_VERTEX"; break;
    case RequestType::HAS_EDGE: out << "HAS_EDGE"; break;
    case RequestType::GET_WEIGHT: out << "GET_WEIGHT"; break;
    case RequestType::LOAD: out << "LOAD"; break;
    case RequestType::ADD_VERTEX: out << "ADD_VERTEX"; break;
    case RequestType::DELETE_VERTEX: out << "REMOVE_VERTEX"; break;
    case RequestType::ADD_EDGE: out << "ADD_EDGE"; break;
    case RequestType::DELETE_EDGE: out << "REMOVE_EDGE"; break;
    case RequestType::BFS_ALL: out << "BFS_ALL"; break;
    case RequestType::BFS_ONE: out << "BFS_ONE"; break;
    case RequestType::SPW_ALL: out << "SPW_ALL"; break;
    case RequestType::SPW_ONE: out << "SPW_ONE"; break;
    default: out << "UNKNOWN (request code: " << (uint32_t) type << ")";
    }
    return out;
}

std::ostream& operator<<(std::ostream& out, const Request& request){
    out << "[REQUEST " << request.type() << ", message size: " << request.message_size();
    switch(request.type()){
    case RequestType::ON_MAIN_INIT:
        out << ", num threads: " << request.get(0);
        break;
    case RequestType::ON_THREAD_INIT:
    case RequestType::ON_THREAD_DESTROY:
        out << ", worker_id: " << request.get(0);
        break;
    case RequestType::HAS_VERTEX:
        out << ", vertex_id: " << request.get(0);
        break;
    case RequestType::HAS_EDGE:
        out << ", source: " << request.get(0) << ", destination: " << request.get(1);
        break;
    case RequestType::ADD_VERTEX:
    case RequestType::DELETE_VERTEX:
        out << ", vertex_id: " << request.get(0);
        break;
    case RequestType::ADD_EDGE:
        out << ", source: " << request.get(0) << ", destination: " << request.get(1)<< ", weight: " << request.get(2);
        break;
    case RequestType::DELETE_EDGE:
        out << ", source: " << request.get(0) << ", destination: " << request.get(1);
        break;
    default:
        ; /* nop */
    }

    out << "]";
    return out;
}

std::ostream& operator<<(std::ostream& out, const Request* request){
    if(request == nullptr){
        out << "[REQUEST nullptr]";
    } else {
        out << *request;
    }
    return out;
}

std::ostream& operator<<(std::ostream& out, ResponseType type){
    switch(type){
    case ResponseType::OK: out << "OK"; break;
    case ResponseType::NOT_SUPPORTED: out << "NOT_SUPPORTED"; break;
    default: out << "UNKNOWN (response code: " << (uint32_t) type << ")";
    }
    return out;
}

std::ostream& operator<<(std::ostream& out, const Response& response){
    out << "[RESPONSE " << response.type() << ", message size: " << response.message_size();

    for(int i = 0, end = response.num_arguments(); i < end; i++){
        out << ", arg[" << i << "]: " << response.get<int64_t>(i);
    }

    out << "]";
    return out;
}

std::ostream& operator<<(std::ostream& out, const Response* response){
    if(response == nullptr){
        out << "[RESPONSE nullptr]";
    } else {
        out << *response;
    }
    return out;
}

} // namespace network
