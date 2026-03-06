CXX = g++-14
CXXFLAGS = -std=c++23 -Wall -Wextra -Wpedantic -I/usr/local/include -Iinclude \
		   -Wshadow -Wconversion -Wsign-conversion -Wformat=2 -Wduplicated-branches \
		   -Wduplicated-cond -Wnull-dereference

# Librairies (libpqxx compilée depuis les sources dans /usr/local/lib)
LDFLAGS  = -L/usr/local/lib -lpqxx -lpq
# Google Test
GTEST_FLAGS = -L/usr/local/lib -lgtest -lgtest_main -pthread

SRC_DIR      = src
INCLUDE_DIR  = include
BUILD_DIR    = build
TEST_DIR     = tests
INT_TEST_DIR = tests/integration

# Fichiers sources et objets (sans main.cpp pour les tests)
SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SRCS))
OBJS_NO_MAIN = $(filter-out $(BUILD_DIR)/main.o, $(OBJS))

# Tests unitaires (tests/)
test_src  = $(wildcard $(TEST_DIR)/test_*.cpp)
test_obj  = $(patsubst $(TEST_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(test_src))
test_exec = $(BUILD_DIR)/run_tests

# Tests d'integration (tests/integration/)
int_test_src  = $(wildcard $(INT_TEST_DIR)/test_*.cpp)
int_test_obj  = $(patsubst $(INT_TEST_DIR)/%.cpp, $(BUILD_DIR)/int_%.o, $(int_test_src))
int_test_exec = $(BUILD_DIR)/run_integration_tests

# Nom de l'exécutable
exec = $(BUILD_DIR)/market_insight

.PHONY: all run clean rebuild test test-integration test-all valgrind

all: $(exec)

$(exec): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# Compilation : chaque .cpp -> .o
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Compilation des tests unitaires : tests/test_*.cpp -> build/*.o
$(BUILD_DIR)/%.o: $(TEST_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Compilation des tests d'integration : tests/integration/test_*.cpp -> build/int_*.o
$(BUILD_DIR)/int_%.o: $(INT_TEST_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Linking des tests unitaires : objets du projet (sans main) + objets tests + gtest
$(test_exec): $(OBJS_NO_MAIN) $(test_obj)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(GTEST_FLAGS)

# Linking des tests d'integration : meme base + objets integration + gtest
$(int_test_exec): $(OBJS_NO_MAIN) $(int_test_obj)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(GTEST_FLAGS)

run: $(exec)
	./$(exec)

test: $(test_exec)
	./$(test_exec)

test-integration: $(int_test_exec)
	./$(int_test_exec)

test-all: test test-integration

valgrind: $(test_exec)
	valgrind --leak-check=full --track-origins=yes --error-exitcode=1 \
		--suppressions=valgrind.supp ./$(test_exec)

clean:
	rm -rf $(BUILD_DIR)/*

rebuild: clean all

# Créer le dossier build si nécessaire
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)
