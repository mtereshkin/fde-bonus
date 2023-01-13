#include "JoinQuery.hpp"
#include "Util.hpp"
#include <iostream>
//----- ----------------------------------------------------------------------
//i  = o_orderkey-->o_custkey = j = 1
//i  = l_orderkey-->l_quantity =j =4
//i  = c_custkey-->c_mktsegment =  j = 6
// vector contains: 0, 1, 4, 6
std::unordered_map<int, std::vector<std::string>> getTable(const std::string& file) {
    std::unordered_map<int, std::vector<std::string>> parsed_table;
    std::ifstream input(file);
    std::string next_string;
    int count;
    int string_count = 0;
    std::stringstream ss;
    std::string buff;
    while(input) {
        getline(input, next_string);
        count = 0;
        ss.str("");
        ss.clear();
        ss << next_string;
        while(ss) {
            getline(ss, buff, '|');
            if (!buff.empty()) {
                if ((count == 0) or (count == 1) or (count == 4) or (count == 6)) parsed_table[string_count].emplace_back(std::move(buff));
            }
            count += 1;
            if (count > 6) break;
        }
        string_count += 1;
    }
    input.close();
    return parsed_table;
}


JoinQuery::JoinQuery(std::string lineitem, std::string order,
                     std::string customer)
{
    std::vector<std::string> data = {lineitem, order, customer};
    std::vector<std::future<std::unordered_map<int,  std::vector<std::string>>>> futures;
    for (int i = 0; i < 3; i++) {
        futures.push_back(std::async(getTable, std::ref(data[i])));
    }
    lineitem_table = futures[0].get();
    order_table = futures[1].get();
    customer_table = futures[2].get();
}
//---------------------------------------------------------------------------

std::unordered_map<std::string, std::string> performFirstJoin(size_t begin_chunk, size_t end_chunk,
                                                                   std::unordered_map<std::string, std::string>& hash_map,
                                                                   std::unordered_map<int, std::vector<std::string>>& info) {
    std::unordered_map<std::string, std::string> result;
    for (size_t i = begin_chunk; i < end_chunk; i++) {
        if (hash_map.find(info[i][1]) != hash_map.end()) result.insert({info[i][0], info[i][1]});
    }
    return result;

}
std::pair<uint64_t, uint64_t> performSecondJoin(size_t begin_chunk, size_t end_chunk,
                                                std::unordered_map<std::string, std::string>& hash_map,
                                                std::unordered_map<int, std::vector<std::string>>& info) {
    uint64_t avg = 0, size = 0;
    for (size_t i = begin_chunk; i < end_chunk; i++) {
        if (hash_map.find(info[i][0]) != hash_map.end()) {
            avg += atoi(info[i][2].c_str());
            size += 1;
        }
    }
    return std::make_pair(avg, size);
}
std::unordered_map<std::string, std::string> performSelection(size_t begin_chunk, size_t end_chunk,
                                                              std::string segment,
                                                              std::unordered_map<int, std::vector<std::string>>& info) {
    std::unordered_map<std::string, std::string> result;
    for (size_t i = begin_chunk; i < end_chunk; i++) {
        if (info[i][3] == segment) result.insert({info[i][0], info[i][3]});
    }
    return result;
}
size_t JoinQuery::avg(const std::string& segmentParam) {
    std::unordered_map<std::string, std::string> order_left, customer_left;
//    for (auto it = begin(customer_table); it != end(customer_table); it++) {
//        if (it->second[3] == segmentParam) customer_left[it->second[0]] = it->second[3];
//    }
    unsigned chunks = std::thread::hardware_concurrency();
    std::vector<std::future<std::unordered_map<std::string, std::string>>> futures_first;
    for (unsigned count = 0; count < chunks; count++) {
        auto begin_chunks = count * customer_table.size()/chunks;
        auto end_chunks = (count+1) * customer_table.size()/chunks;
        futures_first.push_back(std::async(performSelection, begin_chunks, end_chunks, segmentParam, std::ref(customer_table)));
    }

    for (auto &item:futures_first) {
        for (const auto& r: item.get()) {
            customer_left.insert({r.first, r.second});
        }
    }


    std::vector<std::future<std::unordered_map<std::string, std::string>>> futures_second;
    for (unsigned count = 0; count < chunks; count++) {
        auto begin_chunks = count * order_table.size()/chunks;
        auto end_chunks = (count+1) * order_table.size()/chunks;
        futures_second.push_back(std::async(performFirstJoin, begin_chunks, end_chunks, std::ref(customer_left), std::ref(order_table)));
    }
//    for (auto it = begin(order_table); it !=  end(order_table); it++) {
//        if (customer_left.find(it->second[1]) != customer_left.end()) order_left[it->second[0]] = it->second[1];
//    }

    for (auto &item:futures_second) {
        for (const auto& r: item.get()) {
            order_left.insert({r.first, r.second});
        }
    }


    std::vector<std::future<std::pair<uint64_t, uint64_t>>> futures_third;
    for (unsigned count = 0; count < chunks; count++) {
        auto begin_chunks = count * lineitem_table.size()/chunks;
        auto end_chunks = (count+1) * lineitem_table.size()/chunks;
        futures_third.push_back(std::async(performSecondJoin, begin_chunks, end_chunks, std::ref(order_left), std::ref(lineitem_table)));
    }

    uint64_t average = 0;
    uint64_t tup_left = 0;
    for (auto i = 0; i < chunks; i++) {
        auto item = futures_third[i].get();
        average += item.first;
        tup_left += item.second;
        }

//    for (auto it = begin(lineitem_table); it != end(lineitem_table); it++) {
//        if (order_left.find(it->second[0]) != order_left.end()){
//            average += atoi(it->second[2].c_str());
//            tup_left += 1;
//        }
//    }
    average *= 100;
    if (average > 0) {
        return average/tup_left;
    }
    return 0;

}
//---------------------------------------------------------------------------
size_t JoinQuery::lineCount(std::string rel)
{
    std::ifstream relation(rel);
    assert(relation);  // make sure the provided string references a file
    size_t n = 0;
    for (std::string line; std::getline(relation, line);) n++;
    return n;
}
//---------------------------------------------------------------------------
