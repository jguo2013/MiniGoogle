CC = g++
DDBUG = -g 
PLIB  = -pthread
INC = ./src
INCDIRS = -I${INC}
SRC_DIR = ./src
CFLAGS = ${INCDIRS} ${DDBUG} ${PLIB}

all: server

clean:
	rm -f $(SRC_DIR)/*.o server

SRC = $(SRC_DIR)/machine.cc $(SRC_DIR)/task.cc $(SRC_DIR)/server.cc	\
      $(SRC_DIR)/request.cc $(SRC_DIR)/alg.cc $(SRC_DIR)/connection.cc
      
OBJ = $(SRC:.cc=.o)  

$(OBJ): $(SRC_DIR)/%.o: $(SRC_DIR)/%.cc
	$(CC) -c $(CFLAGS) $< -o $@

server : $(SRC_DIR)/server.o $(OBJ)
	$(CC) $(CFLAGS) -o $@ $(OBJ)