//
// Created by Chunwei Liu on 8/23/21.
//

/* Select * from lineitem where l_shipdate < date '1994-01-01' + interval '1' year and l_discount between .06 - 0.01 and .06 + 0.01 and l_quantity < 24 */

#include <iostream>
#include <chrono>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>
#include <arrow/io/file.h>
#include <arrow/api.h>
#include "gandiva/tree_expr_builder.h"
#include "gandiva/projector.h"
#include "gandiva/filter.h"
#include "arrow/chunked_array.h"
#include "gandiva/literal_holder.h"
#include "gandiva/projector.h"

using namespace std;
using namespace std::chrono;

void checkStatus(arrow::Status &status)
{
  if (!status.ok())
  {
    throw status.CodeAsString();
  }
}

int main()
{
  arrow::Status status;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto start_TS = high_resolution_clock::now();
  std::shared_ptr<arrow::io::RandomAccessFile> input = arrow::io::ReadableFile::Open("lineitem_10gb.parquet").ValueOrDie();

  // Open Parquet file reader
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  status = parquet::arrow::OpenFile(input, pool, &arrow_reader);
  checkStatus(status);

  // Read entire file as a single Arrow table
  std::shared_ptr<arrow::Table> table;
  status = arrow_reader->ReadTable(&table);
  auto stop_TS = high_resolution_clock::now();
  checkStatus(status);

//  std::shared_ptr<arrow::Table> table;
//  status = arrow_reader->ReadTable({0, 1, 2}, &table);
//  auto stop_TS = high_resolution_clock::now();
//  checkStatus(status);

  auto number_of_records = table->num_rows();
  /*std::cout << "The total number of rows are:" << "\n";
  std::cout << number_of_records << "\n\n";
  std::cout << "========\n" << "Table Schema:\n" << table->schema()->ToString() << "\n========\n\n";*/

  auto start_QE = high_resolution_clock::now();

  auto chunked_l_extendedprice = table->GetColumnByName("l_extendedprice");
  auto chunked_l_quantity = table->GetColumnByName("l_quantity");
  auto chunked_l_discount = table->GetColumnByName("l_discount");
  auto chunked_l_tax = table->GetColumnByName("l_tax");
  auto chunked_l_linenumber = table->GetColumnByName("l_linenumber");
  auto chunked_l_orderkey = table->GetColumnByName("l_orderkey");
  auto chunked_l_partkey = table->GetColumnByName("l_partkey");
  auto chunked_l_suppkey = table->GetColumnByName("l_suppkey");
  auto chunked_l_returnflag = table->GetColumnByName("l_returnflag");
  auto chunked_l_linestatus = table->GetColumnByName("l_linestatus");
  auto chunked_l_shipdate = table->GetColumnByName("l_shipdate");
  auto chunked_l_receiptdatee = table->GetColumnByName("l_receiptdate");
  auto chunked_l_shipinstruct = table->GetColumnByName("l_shipinstruct");
  auto chunked_l_shipmode = table->GetColumnByName("l_shipmode");
  auto chunked_l_comment = table->GetColumnByName("l_comment");
  auto chunked_l_commitdate = table->GetColumnByName("l_commitdate");

  /*std::cout << "Num of chunks for chunked_l_extendedprice is " << chunked_l_extendedprice->num_chunks() << "\n";
  std::cout << "Num of chunks for chunked_l_discount is " << chunked_l_discount->num_chunks() << "\n";
  std::cout << "Num of chunks for chunked_l_tax is " << chunked_l_tax->num_chunks() << "\n";*/

  auto array_l_extendedprice = chunked_l_extendedprice->chunk(0);
  auto array_l_quantity = chunked_l_quantity->chunk(0);
  auto array_l_discount = chunked_l_discount->chunk(0);
  auto array_l_tax = chunked_l_tax->chunk(0);
  auto array_l_linenumber = chunked_l_linenumber->chunk(0);
  auto array_l_orderkey = chunked_l_orderkey->chunk(0);
  auto array_l_partkey = chunked_l_partkey->chunk(0);
  auto array_l_suppkey = chunked_l_suppkey->chunk(0);
  auto array_l_returnflag = chunked_l_returnflag->chunk(0);
  auto array_l_linestatus = chunked_l_linestatus->chunk(0);
  auto array_l_shipdate = chunked_l_shipdate->chunk(0);
  auto array_l_receiptdate = chunked_l_receiptdatee->chunk(0);
  auto array_l_shipinstruct = chunked_l_shipinstruct->chunk(0);
  auto array_l_shipmode = chunked_l_shipmode->chunk(0);
  auto array_l_comment = chunked_l_comment->chunk(0);
  auto array_l_commitdate = chunked_l_commitdate->chunk(0);

  /*Configuration*/
  auto configuration = gandiva::ConfigurationBuilder::DefaultConfiguration();

  /*Field from the Arrow table*/
  auto var_l_extendedprice = table->schema()->GetFieldByName("l_extendedprice");
  auto var_l_quantity = table->schema()->GetFieldByName("l_quantity");
  auto var_l_discount = table->schema()->GetFieldByName("l_discount");
  auto var_l_tax = table->schema()->GetFieldByName("l_tax");
  auto var_l_linenumber = table->schema()->GetFieldByName("l_linenumber");
  auto var_l_orderkey = table->schema()->GetFieldByName("l_orderkey");
  auto var_l_partkey = table->schema()->GetFieldByName("l_partkey");
  auto var_l_suppkey = table->schema()->GetFieldByName("l_suppkey");
  auto var_l_returnflag = table->schema()->GetFieldByName("l_returnflag");
  auto var_l_linestatus = table->schema()->GetFieldByName("l_linestatus");
  auto var_l_shipdate = table->schema()->GetFieldByName("l_shipdate");
  auto var_l_receiptdate = table->schema()->GetFieldByName("l_receiptdate");
  auto var_l_shipinstruct = table->schema()->GetFieldByName("l_shipinstruct");
  auto var_l_shipmode = table->schema()->GetFieldByName("l_shipmode");
  auto var_l_comment = table->schema()->GetFieldByName("l_comment");
  auto var_l_commitdate = table->schema()->GetFieldByName("l_commitdate");

  auto input_schema = arrow::schema({ var_l_extendedprice, var_l_quantity, var_l_discount, var_l_tax, var_l_linenumber, var_l_orderkey, var_l_partkey, var_l_suppkey, var_l_returnflag,
                                      var_l_linestatus, var_l_shipdate, var_l_receiptdate, var_l_shipinstruct, var_l_shipmode, var_l_comment, var_l_commitdate});

  /*Result field*/
  auto field_res1 = arrow::field("result_id1", var_l_quantity->type());
  auto field_res2 = arrow::field("result_id2", var_l_orderkey->type());
  auto field_res3 = arrow::field("result_id3", var_l_partkey->type());
  auto field_res4 = arrow::field("result_id4", var_l_suppkey->type());
  auto field_res5 = arrow::field("result_id5", var_l_returnflag->type());
  auto field_res6 = arrow::field("result_id6", var_l_linestatus->type());
  auto field_res7 = arrow::field("result_id7", var_l_shipdate->type());
  auto field_res8 = arrow::field("result_id8", var_l_receiptdate->type());
  auto field_res9 = arrow::field("result_id9", var_l_shipinstruct->type());
  auto field_res10 = arrow::field("result_id10", var_l_shipmode->type());
  auto field_res11 = arrow::field("result_id11", var_l_comment->type());
  auto field_res12 = arrow::field("result_id12", var_l_extendedprice->type());
  auto field_res13 = arrow::field("result_id13", var_l_discount->type());
  auto field_res14 = arrow::field("result_id14", var_l_tax->type());
  auto field_res15 = arrow::field("result_id15", var_l_linenumber->type());
  auto field_res16 = arrow::field("result_id16", var_l_commitdate->type());

  /*Create the tree node for all the fields*/
  auto node_l_extendedprice = gandiva::TreeExprBuilder::MakeField(var_l_extendedprice);
  auto node_l_quantity = gandiva::TreeExprBuilder::MakeField(var_l_quantity);
  auto node_l_discount = gandiva::TreeExprBuilder::MakeField(var_l_discount);
  auto node_l_linenumber = gandiva::TreeExprBuilder::MakeField(var_l_linenumber);
  auto node_l_tax = gandiva::TreeExprBuilder::MakeField(var_l_tax);
  auto node_l_orderkey = gandiva::TreeExprBuilder::MakeField(var_l_orderkey);
  auto node_l_partkey = gandiva::TreeExprBuilder::MakeField(var_l_partkey);
  auto node_l_suppkey = gandiva::TreeExprBuilder::MakeField(var_l_suppkey);
  auto node_l_returnflag = gandiva::TreeExprBuilder::MakeField(var_l_returnflag);
  auto node_l_linestatus = gandiva::TreeExprBuilder::MakeField(var_l_linestatus);
  auto node_l_shipdate = gandiva::TreeExprBuilder::MakeField(var_l_shipdate);
  auto node_l_receiptdate = gandiva::TreeExprBuilder::MakeField(var_l_receiptdate);
  auto node_l_shipinstruct = gandiva::TreeExprBuilder::MakeField(var_l_shipinstruct);
  auto node_l_shipmode = gandiva::TreeExprBuilder::MakeField(var_l_shipmode);
  auto node_l_comment = gandiva::TreeExprBuilder::MakeField(var_l_comment);
  auto node_l_commitdate = gandiva::TreeExprBuilder::MakeField(var_l_commitdate);

  /*Create the tree builder expression filter */
  auto node_constant_date = gandiva::TreeExprBuilder::MakeLiteral(9131);
  auto node_cast_date = gandiva::TreeExprBuilder::MakeFunction("CastDate", {node_constant_date }, var_l_shipdate->type());
  auto node_less_than_l_shipdate = gandiva::TreeExprBuilder::MakeFunction("less_than", {node_l_shipdate, node_cast_date }, arrow::boolean());

  double d1 = 0.05, d2 = 0.07;
  auto node_Literal_1 = gandiva::TreeExprBuilder::MakeLiteral(d1);
  auto node_Literal_2 = gandiva::TreeExprBuilder::MakeLiteral(d2);

  auto gt_eq = gandiva::TreeExprBuilder::MakeFunction("greater_than_or_equal_to", {node_l_discount, node_Literal_1}, arrow::boolean());
  auto lt_eq = gandiva::TreeExprBuilder::MakeFunction("less_than_or_equal_to", {node_l_discount, node_Literal_2}, arrow::boolean());

  auto node_and_1 = gandiva::TreeExprBuilder::MakeAnd({node_less_than_l_shipdate, gt_eq});
  auto node_and_2 = gandiva::TreeExprBuilder::MakeAnd({node_and_1, lt_eq});

  double d3 = 24.0;
  auto literal_24 = gandiva::TreeExprBuilder::MakeLiteral(d3);
  auto less_than_24 = gandiva::TreeExprBuilder::MakeFunction("less_than", {node_l_quantity, literal_24}, arrow::boolean());

  auto node_and_3 = gandiva::TreeExprBuilder::MakeAnd({node_and_2, less_than_24});
  auto condition = gandiva::TreeExprBuilder::MakeCondition(node_and_3);

  /*Create the tree builder expression */
  auto expr1 = gandiva::TreeExprBuilder::MakeExpression(node_l_quantity, field_res1);
  auto expr2 = gandiva::TreeExprBuilder::MakeExpression(node_l_orderkey, field_res2);
  auto expr3 = gandiva::TreeExprBuilder::MakeExpression(node_l_partkey, field_res3);
  auto expr4 = gandiva::TreeExprBuilder::MakeExpression(node_l_suppkey, field_res4);
  auto expr5 = gandiva::TreeExprBuilder::MakeExpression(node_l_returnflag, field_res5);
  auto expr6 = gandiva::TreeExprBuilder::MakeExpression(node_l_linestatus, field_res6);
  auto expr7 = gandiva::TreeExprBuilder::MakeExpression(node_l_shipdate, field_res7);
  auto expr8 = gandiva::TreeExprBuilder::MakeExpression(node_l_receiptdate, field_res8);
  auto expr9 = gandiva::TreeExprBuilder::MakeExpression(node_l_shipinstruct, field_res9);
  auto expr10 = gandiva::TreeExprBuilder::MakeExpression(node_l_shipmode, field_res10);
  auto expr11 = gandiva::TreeExprBuilder::MakeExpression(node_l_comment, field_res11);
  auto expr12 = gandiva::TreeExprBuilder::MakeExpression(node_l_extendedprice, field_res12);
  auto expr13 = gandiva::TreeExprBuilder::MakeExpression(node_l_discount, field_res13);
  auto expr14 = gandiva::TreeExprBuilder::MakeExpression(node_l_tax, field_res14);
  auto expr15 = gandiva::TreeExprBuilder::MakeExpression(node_l_linenumber, field_res15);
  auto expr16 = gandiva::TreeExprBuilder::MakeExpression(node_l_commitdate, field_res16);

  /* std::cout << "\n======= tree builder expression ===============\n";
  std::cout << condition->ToString() << "\n"; */

  auto in_batch = arrow::RecordBatch::Make(input_schema, number_of_records, {array_l_extendedprice, array_l_quantity, array_l_discount, array_l_tax, array_l_linenumber,
                                                                             array_l_orderkey, array_l_partkey, array_l_suppkey, array_l_returnflag, array_l_linestatus, array_l_shipdate, array_l_receiptdate,
                                                                             array_l_shipinstruct, array_l_shipmode, array_l_comment, array_l_commitdate});

  /*Create the Selector*/
  std::shared_ptr<gandiva::Filter> filter;
  status = gandiva::Filter::Make(input_schema, condition, configuration, &filter);
  checkStatus(status);

  std::shared_ptr<gandiva::SelectionVector> selection_vector;
  status = gandiva::SelectionVector::MakeInt64(number_of_records, pool, &selection_vector);
  checkStatus(status);

  status = filter->Evaluate(*in_batch, selection_vector);
  checkStatus(status);

  /*Create the Projector*/
  std::shared_ptr<gandiva::Projector> projector;
  status = gandiva::Projector::Make(input_schema, {expr1, expr2, expr3, expr4, expr5, expr6, expr7, expr8, expr9, expr10, expr11, expr12, expr13, expr14, expr15,
                                                   expr16}, gandiva::SelectionVector::MODE_UINT64, configuration, &projector);
  checkStatus(status);

  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, selection_vector.get(), pool, &outputs);
  checkStatus(status);

  auto stop_QE = high_resolution_clock::now();

  auto duration_TS = duration_cast<microseconds>(stop_TS - start_TS);
  cout << "\nStart_TS: " << duration_cast<microseconds>(start_TS.time_since_epoch()).count() << endl;
  cout << "\nEnd_TS: " << duration_cast<microseconds>(stop_TS.time_since_epoch()).count() << endl;
  cout << "File Read Time: " << duration_TS.count() << " microseconds" << endl;

  auto duration_QE = duration_cast<microseconds>(stop_QE - start_QE);
  cout << "\nStart_QE: " << duration_cast<microseconds>(start_QE.time_since_epoch()).count() << endl;
  cout << "\nEnd_QE: " << duration_cast<microseconds>(stop_QE.time_since_epoch()).count() << endl;
  cout << "Time taken by query: " << duration_QE.count() << " microseconds" << endl;

  std::cout << "========\n" << "Table Schema:\n" << table->schema()->ToString() << "\n========\n\n";
  std::cout << "Num of rows : " << number_of_records << std::endl;
  std::cout << "========\n" << "Result:\n" << outputs[0]->ToString() << "\n========\n\n";

  std::cout << "\ndone\n";
  return 1;
}