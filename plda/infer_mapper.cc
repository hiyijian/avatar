#include <set>
#include <fstream>
#include <sstream>
#include <string>
#include "common.h"
#include "document.h"
#include "model.h"
#include "sampler.h"
#include "cmd_flags.h"

int main(int argc, char** argv) {
  using learning_lda::LDACorpus;
  using learning_lda::LDAModel;
  using learning_lda::LDAAccumulativeModel;
  using learning_lda::LDASampler;
  using learning_lda::LDADocument;
  using learning_lda::LDACmdLineFlags;
  using learning_lda::DocumentWordTopicsPB;
  using learning_lda::RandInt;
  using std::ifstream;
  using std::istringstream;

  map<string, int> word_index_map;
  string line;
  while (getline(std::cin, line)) {  // Each line is a training document.
    if (line.size() > 0 &&      // Skip empty lines.
        line[0] != '\r' &&      // Skip empty lines.
        line[0] != '\n' &&      // Skip empty lines.
        line[0] != '#') {       // Skip comment lines.
		std::cout << line << "\n";
	}
  }
}
