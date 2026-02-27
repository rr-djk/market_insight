CXX = g++
CXXFLAGS = -std=c++23 -Wall -Wextra -Wpedantic -I/usr/local/include -Iinclude \
		   -Wshadow -Wconversion -Wsign-conversion -Wformat=2 -Wduplicated-branches \
		   -Wduplicated-cond -Wnull-dereference

# Librairies (libpqxx compilée depuis les sources dans /usr/local/lib)
LDFLAGS  = -L/usr/local/lib -lpqxx -lpq
# Google Test
GTEST_FLAGS = -L/usr/local/lib -lgtest -lgtest_main -pthread

SRC_DIR     = src
INCLUDE_DIR = include
BUILD_DIR   = build
TEST_DIR    = tests

# Fichiers sources et objets (sans main.cpp pour les tests)
SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SRCS))
OBJS_NO_MAIN = $(filter-out $(BUILD_DIR)/main.o, $(OBJS))

# Fichiers de tests
test_src = $(wildcard $(TEST_DIR)/test_*.cpp)
test_obj = $(patsubst $(TEST_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(test_src))
test_exec = $(BUILD_DIR)/run_tests

# Nom de l'exécutable
exec = $(BUILD_DIR)/market_insight

.PHONY: all run clean rebuild test

all: $(exec)

$(exec): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# Compilation : chaque .cpp -> .o
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Compilation des tests : chaque test_*.cpp -> .o
$(BUILD_DIR)/%.o: $(TEST_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Linking des tests : objets du projet (sans main) + objets des tests + gtest
$(test_exec): $(OBJS_NO_MAIN) $(test_obj)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(GTEST_FLAGS)

run: $(exec)
	./$(exec)

test: $(test_exec)
	./$(test_exec)

clean:
	rm -rf $(BUILD_DIR)/*

rebuild: clean all

# Créer le dossier build si nécessaire
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)
