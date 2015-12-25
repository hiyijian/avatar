CC=g++-4.1
MPICC=mpicxx

CFLAGS=-O3 -Wall -Wno-sign-compare

all: lda mpi_lda infer infer_mapper infer_reducer

clean:
	rm -f *.o
	rm -f lda mpi_lda infer infer_mapper infer_reducer

cmd_flags.o: cmd_flags.cc cmd_flags.h
	$(CC) -c $(CFLAGS)  cmd_flags.cc -o cmd_flags.o

common.o: common.cc common.h
	$(CC) -c $(CFLAGS)  common.cc -o common.o

document.o: document.cc document.h common.o
	$(CC) -c $(CFLAGS)  document.cc -o document.o

model.o: model.cc model.h common.o
	$(CC) -c $(CFLAGS)  model.cc -o model.o

accumulative_model.o: accumulative_model.cc accumulative_model.h common.o model.o
	$(CC) -c $(CFLAGS)  accumulative_model.cc -o accumulative_model.o

sampler.o: sampler.cc sampler.h common.o document.o model.o accumulative_model.o
	$(CC) -c $(CFLAGS)  sampler.cc -o sampler.o

lda: lda.cc cmd_flags.o common.o document.o model.o accumulative_model.o sampler.o
	$(CC) $(CFLAGS) lda.cc cmd_flags.o common.o document.o model.o accumulative_model.o sampler.o -o lda

infer: infer.cc cmd_flags.o common.o document.o model.o accumulative_model.o sampler.o
	$(CC) $(CFLAGS) infer.cc cmd_flags.o common.o document.o model.o accumulative_model.o sampler.o -o infer

infer_mapper: infer_mapper.cc cmd_flags.o common.o
	$(CC) $(CFLAGS) infer_mapper.cc cmd_flags.o common.o -o infer_mapper

infer_reducer: infer_reducer.cc cmd_flags.o common.o document.o model.o accumulative_model.o sampler.o
	$(CC) $(CFLAGS) infer_reducer.cc cmd_flags.o common.o document.o model.o accumulative_model.o sampler.o -o infer_reducer

mpi_lda: mpi_lda.cc cmd_flags.o common.o document.o model.o accumulative_model.o sampler.o
	$(MPICC) $(CFLAGS) mpi_lda.cc cmd_flags.o common.o document.o model.o accumulative_model.o sampler.o -o mpi_lda
