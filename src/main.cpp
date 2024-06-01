//
// Created by jub on 12.05.24.
//


#include <iostream>
#include "XlsxFile.h"
#include <filesystem>
#include <unordered_set>
#include <vector>

std::vector<std::pair<uInt ,LocationInfo>> findDuplicates(const std::vector<std::pair<uInt ,LocationInfo>>& locs) {
    std::unordered_set<int> numSet;
    std::vector<std::pair<uInt ,LocationInfo>> duplicates;

    for (const auto& loc : locs) {
        if (numSet.count(loc.second.row)) {
            duplicates.push_back(loc);
        } else {
            numSet.insert(loc.second.row);
        }
    }

    return duplicates;
}

int main() {
    std::cout << "Hello, World!" << std::endl;
    // Print current working directory
    system("pwd");
    try {

        XlsxFile file("../50k.xlsx");
        file.mParallelStrings = false;
        file.parseSharedStrings();

        auto sheetNumber = 1;
        auto sheetName = "";
        XlsxSheet fsheet = file.getSheet(sheetNumber);
        fsheet.mHeaders = true;
        // if parallel we need threads for string parsing
        // if "efficient", both sheet & strings need additional thread for decompression (meaning min is 2)
//        int act_num_threads = num_threads - parallel * 2 - (num_threads > 1);
//        if (act_num_threads <= 0) act_num_threads = 1;
        int act_num_threads = 6;
        int skip_rows = 1;
        int skip_columns = 0;
        bool success = fsheet.interleaved(skip_rows, skip_columns, act_num_threads);
        file.finalize();

        // One way to get the data (per row)
        // Initialize the nextRow()
        // First location is always empty & fsheet.mLocationInfos[1] is first row + same for nextRow()
        fsheet.nextRow();
        // Loop through the rows
        auto locations = fsheet.mLocationInfos[0];
        auto firstLocation = locations[352];

        // Iterate through chunks
//        std::vector<std::vector<XlsxCell>> result;
//        for(auto& chunk : fsheet.mCells) {
//            for(auto& row : chunk) {
//                std::vector<XlsxCell> newRow;
//                for(auto& cell : row) {
//                    newRow.push_back(cell);
//                }
//                result.push_back(newRow);
//            }
//        }

        // Result vector
        std::vector<std::vector<XlsxCell>> result;

        // Find row with value -1ul
        std::vector<std::pair<uInt ,LocationInfo>> locWithMaxVal ;
        auto currentThread = 0;
        for (auto thread : fsheet.mLocationInfos) {
            for (auto location : thread) {
                if (location.row == -1ul) {
                    locWithMaxVal.push_back(std::make_pair(currentThread, location));
                }
            }
            currentThread++;
        }
        std::vector<std::pair<uInt ,LocationInfo>> allLocs;
        currentThread = 0;
        for (auto thread : fsheet.mLocationInfos) {
            for (auto location : thread) {
                    allLocs.push_back(std::make_pair(currentThread, location));
            }
            currentThread++;
        }

        // Find duplicates
        auto duplicateLocations = findDuplicates(allLocs);

        // Gather columns for all locations
        std::vector<std::pair<uInt ,LocationInfo>> locationsWithOffset ;
        currentThread = 0;
        for (auto thread : fsheet.mLocationInfos) {
            for (auto location : thread) {
                if (location.column == 0) {
                    continue;
                }
                locationsWithOffset.push_back(std::make_pair(currentThread, location));
            }
            currentThread++;
        }



        // Find locations with double row
        currentThread = 0;
        std::map<uInt ,std::vector<std::pair<uInt ,LocationInfo>>> locationsWithDoubleRow ;
        for (auto thread : fsheet.mLocationInfos) {
            for (auto location : thread) {
                for (auto loc : locationsWithOffset) {
                    if (loc.second.row == location.row) {
                        locationsWithDoubleRow[location.row].push_back(std::make_pair(currentThread,location));

                    }
                }
            }
            currentThread++;
        }

        // Save locations in order by row
        std::vector<std::pair<uInt ,LocationInfo>> locationsInOrder;
        int currentRow = 0;
        std::vector<int> current_pos(fsheet.mLocationInfos.size(), 0);
        while (currentRow < fsheet.mDimension.second) {
            currentThread = 0;
            for (auto thread_locs: fsheet.mLocationInfos) {

                if (current_pos[currentThread] <= thread_locs.size()) {
                    currentRow = thread_locs[current_pos[currentThread]].row;
                } else {
                    currentThread++;
                    continue;
                }
                for (; current_pos[currentThread] < thread_locs.size(); current_pos[currentThread]++) {
                    auto location = thread_locs[current_pos[currentThread]];
                    if (location.row - currentRow > 1) {
                        break;
                    }
                    locationsInOrder.push_back(std::make_pair(currentThread, location));
                    currentRow = location.row;
                }
                currentThread++;
            }
        }

        // Looks nicer but is afaik less efficient
        std::vector<std::pair<uInt ,LocationInfo>> orderedLocations;
        std::vector<int> currentPositions(fsheet.mLocationInfos.size(), 0);

        for (int currentRowNumber = 0; currentRowNumber < fsheet.mDimension.second; ++currentRowNumber) {
            for (int currentThreadIndex = 0; currentThreadIndex < fsheet.mLocationInfos.size(); ++currentThreadIndex) {
                auto& locationsInCurrentThread = fsheet.mLocationInfos[currentThreadIndex];

                // Skip if current position is out of range
                if (currentPositions[currentThreadIndex] >= locationsInCurrentThread.size()) continue;

                // Skip if the row of the current location is not the current row
                if (locationsInCurrentThread[currentPositions[currentThreadIndex]].row != currentRowNumber) continue;

                // Add locations from the current thread until row is not current anymore
                while (currentPositions[currentThreadIndex] < locationsInCurrentThread.size() && locationsInCurrentThread[currentPositions[currentThreadIndex]].row - currentRowNumber <= 1) {
                    orderedLocations.push_back(std::make_pair(currentThreadIndex, locationsInCurrentThread[currentPositions[currentThreadIndex]]));
                    ++currentPositions[currentThreadIndex];
                }
            }
        }
        // Autopilot rewrite -- looses one location info (the last one)
//        std::vector<std::pair<uInt ,LocationInfo>> locationsInOrder2;
//        std::vector<int> current_pos2(fsheet.mLocationInfos.size(), 0);
//
//        for (int currentRow = 0; currentRow < fsheet.mDimension.second; ++currentRow) {
//            for (int currentThread = 0; currentThread < fsheet.mLocationInfos.size(); ++currentThread) {
//                auto& thread_locs = fsheet.mLocationInfos[currentThread];
//
//                // Skip if current position is out of range
//                if (current_pos2[currentThread] >= thread_locs.size()) continue;
//
//                // Skip if the row of the current location is not the current row
//                if (thread_locs[current_pos2[currentThread]].row != currentRow) continue;
//
//                // Add all locations of the current row to the ordered list
//                while (current_pos2[currentThread] < thread_locs.size() && thread_locs[current_pos2[currentThread]].row == currentRow) {
//                    locationsInOrder2.push_back(std::make_pair(currentThread, thread_locs[current_pos2[currentThread]]));
//                    ++current_pos2[currentThread];
//                }
//            }
//        }

        while (true) {
            auto row = fsheet.nextRow();
            if (row.first == 0) {
                break;
            }
            result.push_back(row.second);
        }

        if (!success) {
            std::cout << "Error" << std::endl;
            return 1;
        }
        auto row = fsheet.nextRow();

        std::cout << "Success" << std::endl;
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
