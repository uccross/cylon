/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <table.hpp>
#include <status.hpp>
#include <iostream>
#include <io/csv_read_config.h>

int main(int argc, char *argv[]) {
  std::shared_ptr<twisterx::Table> table1;
  auto status2 = twisterx::Table::FromCSV("/tmp/csv.csv", table1);

  std::vector<std::shared_ptr<twisterx::Table>> outs;

  table1->Print();

  table1->HashPartition({1, 2, 3}, 4, &outs);

  for (auto t: outs) {
	std::cout << "-----" << std::endl;
    t->Print();
  }

  return 0;
}


