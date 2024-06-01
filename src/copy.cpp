#include <iostream>
#include "XlsxFile.h"
#include <filesystem>
#include <cmath>


std::vector<std::vector<XlsxCell>> copy() {
    std::cout << "Hello, World!" << std::endl;
    // Print current working directory
    system("pwd");
    try {

        XlsxFile file("../50k.xlsx");
        file.mParallelStrings = false;
        file.parseSharedStrings();

        auto sheetNumber = 1;
        auto sheetName = "";
        XlsxSheet sheet = file.getSheet(sheetNumber);
        sheet.mHeaders = true;
        // if parallel we need threads for string parsing
        // if "efficient", both sheet & strings need additional thread for decompression (meaning min is 2)
//        int act_num_threads = num_threads - parallel * 2 - (num_threads > 1);
//        if (act_num_threads <= 0) act_num_threads = 1;
        int act_num_threads = 4;
        int skip_rows = 1;
        int skip_columns = 0;
        bool success = sheet.interleaved(skip_rows, skip_columns, act_num_threads);
        if (success)
            std::cout << "Success parsing" << std::endl;
        file.finalize();

        std::vector<std::vector<XlsxCell>> tuple;

        size_t nColumns = sheet.mDimension.first;
        size_t nRows = sheet.mDimension.second;
        nRows = nRows - sheet.mHeaders - sheet.mSkipRows;
        nColumns = nColumns - sheet.mSkipColumns;
        if (nRows == 0) {
            return tuple;
        }

        std::vector<XlsxCell> proxies;
        std::vector<std::tuple<XlsxCell, CellType, size_t>> headerCells;
        if (nColumns > 0) {
            proxies.reserve(nColumns);
            headerCells.reserve(nColumns);
        }
        std::vector<CellType> coltypes(nColumns, CellType::T_NONE);
        //! TODO: Afaik this is only once overwritten with T_NONE at (47) -- used in R but not Python version
        std::vector<CellType> coerce(nColumns, CellType::T_NONE);

        std::vector<size_t > d1 = {nRows};
        const double nanv = nan("");

        unsigned long currentColumn = 0;
        long long currentRow = -1;

        const size_t number_threads = sheet.mCells.size();

        std::vector<size_t> currentLocs(number_threads, 0);
        const size_t maxBuffers = number_threads > 0 ? sheet.mCells[0].size() : 0;
        for (size_t buf = 0; buf < maxBuffers; ++buf) {
            for (size_t ithread = 0; ithread < number_threads; ++ithread) {
                if (sheet.mCells[ithread].size() == 0) {
                    break;
                }
                //std::cout << buf << ", " << ithread << "/" << number_threads << std::endl;
                //! Cells for current buffer in current thread
                const std::vector<XlsxCell> cells = sheet.mCells[ithread].front();
                //! Location info for current thread
                const std::vector<LocationInfo>& locs = sheet.mLocationInfos[ithread];
                // ! Current index in location infos for current thread
                size_t& currentLoc = currentLocs[ithread];

                // TODO: Find out when the following is the case
                // icell <= cells.size() because there might be location info after last cell
                //! Index of current cell in buffer
                for (size_t icell = 0; icell <= cells.size(); ++icell) {
                    //! Update currentRow & currentColumn when location info is available for current cell
                    //! After setting those values: Advance to next location info
                    // TODO: Find out when this is executed > 1 times
                    size_t loop_executions = 0;
                    while (currentLoc < locs.size() && locs[currentLoc].buffer == buf && locs[currentLoc].cell == icell) {
                        //std::cout << "loc " << currentLoc << "/" << locs.size() << ": " << locs[currentLoc].buffer << " vs " << buf << ", " << locs[currentLoc].cell << " vs " << icell << " (" << locs[currentLoc].column << "/" << locs[currentLoc].row << ")" << std::endl;
                        currentColumn = locs[currentLoc].column;
                        if (locs[currentLoc].row == -1ul) {
                            ++currentRow;
                        } else {
                            currentRow = locs[currentLoc].row;
                        }
                        // Increment for next iteration
                        ++currentLoc;
                        ++loop_executions;
                    }
                    if (loop_executions > 1) {
                        std::cout << "Loop executed " << loop_executions << " times" << std::endl;
                    }
                    if (icell >= cells.size()) break;
                    const auto adjustedColumn = currentColumn;
                    const auto adjustedRow = currentRow - sheet.mSkipRows;
                    //! Current cell from buffer
                    const XlsxCell& cell = cells[icell];
                    const CellType type = cell.type;

                    //std::cout << adjustedColumn << "/" << adjustedRow << std::endl;

                    if (adjustedRow >= sheet.mHeaders) {
                        // normal (non-header) cell
                        //! If we find a cell whose column is greater than the current number of column types,
                        //! we need to add a new column type with T_NONE

                        if (coltypes.size() <= adjustedColumn) {
                            coltypes.resize(adjustedColumn + 1, CellType::T_NONE);
                            ///! (47)
                            coerce.resize(adjustedColumn + 1, CellType::T_NONE);
                        }
                        //! If the column type is T_NONE or T_ERROR
                        //! (1) ???
                        //! (2) Create a new proxy object with the correct type
                        //! (3)
                        //! (4) Set the column type to the correct type
                        if (coltypes[adjustedColumn] == CellType::T_NONE || coltypes[adjustedColumn] == CellType::T_ERROR) {
                            PyArrayObject* robj = reinterpret_cast<PyArrayObject*>(PyArray_SimpleNew(1, d1, NPY_DOUBLE));
                            for (unsigned long i = 0; i < nRows; ++i) *reinterpret_cast<double*>(PyArray_GETPTR1(robj, i)) = nanv;
                            if (type == CellType::T_NUMERIC) {
                                //NOOP
                            } else if (type == CellType::T_STRING_REF || type == CellType::T_STRING || type == CellType::T_STRING_INLINE) {
                                robj = reinterpret_cast<PyArrayObject*>(PyArray_SimpleNew(1, d1, NPY_OBJECT));
                            } else if (type == CellType::T_BOOLEAN) {
                                robj = reinterpret_cast<PyArrayObject*>(PyArray_SimpleNew(1, d1, NPY_BOOL));
                            } else if (type == CellType::T_DATE) {
                                PyObject *date_type = Py_BuildValue("s", "M8[ms]");
                                PyArray_Descr *descr;
                                PyArray_DescrConverter(date_type, &descr);
                                Py_XDECREF(date_type);

                                robj = reinterpret_cast<PyArrayObject*>(PyArray_SimpleNewFromDescr(1, d1, descr));
                                for (unsigned long i = 0; i < nRows; ++i) *reinterpret_cast<long long*>(PyArray_GETPTR1(robj, i)) = NPY_DATETIME_NAT;
                            }
                            //! If type of cell is not T_NONE, adjust column type to this type
                            if (type != CellType::T_NONE && robj != nullptr) {
                                if (proxies.size() < adjustedColumn) {
                                    proxies.reserve(adjustedColumn + 1);
                                    proxies.resize(adjustedColumn);
                                    proxies.push_back(robj);
                                } else if (proxies.size() > adjustedColumn) {
                                    proxies[adjustedColumn] = robj;
                                } else {
                                    proxies.push_back(robj);
                                }
                                //! (4)
                                coltypes[adjustedColumn] = type;
                            }
                        }
                        if (coltypes[adjustedColumn] != CellType::T_NONE && type != CellType::T_NONE && type != CellType::T_ERROR) {
                            const CellType col_type = coltypes[adjustedColumn];
                            //! If the type of the cell is compatible with the column type
                            //! - Either the types are the same
                            //! - Both are strings (T_STRING_REF, T_STRING, T_STRING_INLINE)
                            const bool compatible = ((type == col_type)
                                                     || (type == CellType::T_STRING_REF && col_type == CellType::T_STRING)
                                                     || (type == CellType::T_STRING_REF && col_type == CellType::T_STRING_INLINE)
                                                     || (type == CellType::T_STRING && col_type == CellType::T_STRING_REF)
                                                     || (type == CellType::T_STRING && col_type == CellType::T_STRING_INLINE)
                                                     || (type == CellType::T_STRING_INLINE && col_type == CellType::T_STRING_REF)
                                                     || (type == CellType::T_STRING_INLINE && col_type == CellType::T_STRING));
                            const unsigned long rowNumber = adjustedRow - sheet.mHeaders;
                            //! (48) Never the case in the python Version -- In R version, coerceString converts the cell
                            //! to a string from T_NUMERIC, T_BOOLEAN, T_DATE...
                            if (coerce[adjustedColumn] == CellType::T_STRING) {
                                PyArrayObject* robj = proxies[adjustedColumn];
                                //coerceString(file, ithread, robj, rowNumber, cell, type);
                            } else if (compatible) {
                                PyArrayObject* robj = proxies[adjustedColumn];
                                if (type == CellType::T_NUMERIC) {
                                    *reinterpret_cast<double*>(PyArray_GETPTR1(robj, rowNumber)) = cell.data.real;
                                } else if (type == CellType::T_STRING_REF) {
                                    auto* str = file.getString(cell.data.integer);
                                    *reinterpret_cast<PyObject**>(PyArray_GETPTR1(robj, rowNumber)) = str;
                                } else if (type == CellType::T_STRING || type == CellType::T_STRING_INLINE) {
                                    auto& str = file.getDynamicString(ithread, cell.data.integer);
                                    *reinterpret_cast<PyObject**>(PyArray_GETPTR1(robj, rowNumber)) = PyUnicode_FromString(str.c_str());
                                } else if (type == CellType::T_BOOLEAN) {
                                    *reinterpret_cast<bool*>(PyArray_GETPTR1(robj, rowNumber)) = cell.data.boolean;
                                } else if (type == CellType::T_DATE) {
                                    *reinterpret_cast<long long*>(PyArray_GETPTR1(robj, rowNumber)) = cell.data.real * 1000;
                                }
                                //! If cell type is not compatible with column type, this case is reached
                                //! This only happens once per column because `coerce[adjustedColumn] = CellType::T_STRING;`
                                //! so the first branch will be taken in the future `if(coerce[adjustedColumn] == CellType::T_STRING)` (48)
                                //! All rows all the column are converted to strings in this branch
                            } else if (coerce[adjustedColumn] == CellType::T_NONE) {
                                /*coerce[adjustedColumn] = CellType::T_STRING;
                                if (col_type != CellType::T_STRING && col_type != CellType::T_STRING_REF && col_type != CellType::T_STRING_INLINE) {
                                    // convert existing
                                    Rcpp::RObject& robj = proxies[adjustedColumn];
                                    Rcpp::RObject newObj = Rcpp::CharacterVector(nRows, Rcpp::CharacterVector::get_na());
                                    for (size_t rowNumber = 0; rowNumber < nRows; ++rowNumber) {
                                        if (col_type == CellType::T_NUMERIC) {
                                            if (Rcpp::NumericVector::is_na(static_cast<Rcpp::NumericVector>(robj)[rowNumber])) continue;
                                            static_cast<Rcpp::CharacterVector>(newObj)[rowNumber] = formatNumber(static_cast<Rcpp::NumericVector>(robj)[rowNumber]);
                                        } else if (col_type == CellType::T_BOOLEAN) {
                                            if (Rcpp::LogicalVector::is_na(static_cast<Rcpp::LogicalVector>(robj)[rowNumber])) continue;
                                            static_cast<Rcpp::CharacterVector>(newObj)[rowNumber] = static_cast<Rcpp::LogicalVector>(robj)[rowNumber] ? "TRUE" : "FALSE";
                                        } else if (col_type == CellType::T_DATE) {
                                            if (Rcpp::DatetimeVector::is_na(static_cast<Rcpp::DatetimeVector>(robj)[rowNumber])) continue;
                                            static_cast<Rcpp::CharacterVector>(newObj)[rowNumber] = formatDatetime(static_cast<Rcpp::DatetimeVector>(robj)[rowNumber]);
                                        }
                                    }
                                    proxies[adjustedColumn] = newObj;
                                }
                                coerceString(file, ithread, proxies[adjustedColumn], rowNumber, cell, type);*/
                            }
                        }
                    } else {
                        // header cell
                        if (headerCells.size() <= adjustedColumn) {
                            headerCells.resize(adjustedColumn + 1);
                        }
                        headerCells[adjustedColumn] = std::make_tuple(cell, type, ithread);
                    }
                    ++currentColumn;
                }
                sheet.mCells[ithread].pop_front();
            }
        }

        // Set headers
        size_t numCols = std::max(proxies.size(), headerCells.size());
        //! Probably dict of header names
        PyObject* dict = PyDict_New();

        for (size_t i = 0; i < numCols; ++i) {
            // header
            std::string colName = "Column" + std::to_string(i);
            if (i < headerCells.size() && std::get<1>(headerCells[i]) != CellType::T_NONE) {
                auto& cell = std::get<0>(headerCells[i]);
                auto& type = std::get<1>(headerCells[i]);
                if (type == CellType::T_NUMERIC) {
                    colName = cell.data.real;
                } else if (type == CellType::T_STRING_REF) {
                    colName = PyUnicode_AsUTF8(file.getString(cell.data.integer));
                } else if (type == CellType::T_STRING || type == CellType::T_STRING_INLINE) {
                    colName = file.getDynamicString(std::get<2>(headerCells[i]), cell.data.integer);
                } else if (type == CellType::T_BOOLEAN) {
                    colName = cell.data.boolean;
                } else if (type == CellType::T_DATE) {
                    colName = cell.data.real;
                }
            }
            // data
            if (i < proxies.size()) {
                PyDict_SetItemString(dict, colName.c_str(), reinterpret_cast<PyObject*>(proxies[i]));
            } else {
                PyArrayObject* robj = reinterpret_cast<PyArrayObject*>(PyArray_SimpleNew(1, d1, NPY_DOUBLE));
                for (unsigned long i = 0; i < nRows; ++i) *reinterpret_cast<double*>(PyArray_GETPTR1(robj, i)) = nanv;
                PyDict_SetItemString(dict, colName.c_str(), reinterpret_cast<PyObject*>(robj));
            }
        }

        PyTuple_SetItem(tuple, 0, dict);
        return tuple;


        std::cout << "Success" << std::endl;
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
