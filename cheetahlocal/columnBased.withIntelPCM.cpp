//g++ -O testIntel.cpp /home/ali/IntelPerformanceCounterMonitorV2.6/cpucounters.cpp /home/ali/IntelPerformanceCounterMonitorV2.6/msr.cpp /home/ali/IntelPerformanceCounterMonitorV2.6/pci.cpp /home/ali/IntelPerformanceCounterMonitorV2.6/client_bw.cpp -o 8-4 -I /home/ali/IntelPerformanceCounterMonitorV2.6 -lpthread -o test.o

#include <iostream>
#include <fstream>
#include <vector>
#include "cpucounters.h"
#include <ctime>
#include <stdlib.h>
#include <sys/time.h>

using namespace std;
enum ExpType {
	SELECT_SUM_A,
	SELECT_SUM_B_WHERE_A,
	SELECT_SUM_AB_a,
	SELECT_SUM_AB_b_WHERE_AB_a
};

struct CustomCoreEventDescription {
	int32 event_number, umask_value;
};

int main(int argc, char *argv[]) {
	// send the following parameters to this program: numberOfRow  numberOfColumns selectivity

	const int programVer = 12;
	/*
	 * 3: select sum(y,z) where x<selectivity
	 * 4: like 3, only AB (row based) has the same values of column based (a)
	 * 5: like 4, but sum(all columns)
	 * 6: like 5, but replace "double" with "long double"
	 * 7: like 5, but replace "double" with "float"
	 * 8: Block Memory
	 * 9: based on 5; simple sum on all elements just select sum(*) to see if there is a jump in col=32 on the col-based
	 * 12: column/row based-standalone implementation
	 */

	int numRows = 1 * 1000 * 1000;
	int numCols = 10;
	double selectivity = 0.1;

	volatile double sum = 0.0;

	if (argc == 4) {
		numRows = atoi(argv[1]);
		numCols = atoi(argv[2]);
		selectivity = atof(argv[3]);
	}

	//initilize random number generator
	srand(time(NULL));

	//Select a random column for the condition cluase
	int A_col_id = rand() % numCols; //A is the column on which we do the search
	int AbaseIndex = A_col_id * numRows;

	//Initialize the data
	int totalNumberOfElements = numCols * numRows;
	double * db = new double[totalNumberOfElements];
	double temp = 0.0;
	//temp=1.0/7.0;
	for (int i = 0; i < totalNumberOfElements; i += 4) {
		//              db[i] = ((double) rand()) / RAND_MAX;
		temp = ((double) rand()) / RAND_MAX;
		//use one random number for every 4 fields; to save initialization time!
		db[i] = temp;
		db[i + 1] = temp;
		db[i + 2] = temp;
		db[i + 3] = temp;
	}

	// select sum(*) where db.column[A_col_id]<=selctivity
	sum = 0.0;
	PCM * m = PCM::getInstance();
	PCM::CustomCoreEventDescription MyEvents[25]; // only 4 fully programmable counters are supported on microarchitecture codenamed Nehalem/Westmere
	MyEvents[0].event_number = 0x24;MyEvents[0].umask_value = 0x01;
	MyEvents[1].event_number = 0x24;MyEvents[1].umask_value = 0x03;
	MyEvents[2].event_number = 0x51;MyEvents[2].umask_value = 0x01;
	MyEvents[3].event_number = 0x51;MyEvents[3].umask_value = 0x04;
	MyEvents[4].event_number = 0x51;MyEvents[4].umask_value = 0x08;
	MyEvents[5].event_number = 0xF0;MyEvents[5].umask_value = 0x01;
	MyEvents[6].event_number = 0xF2;MyEvents[6].umask_value = 0x01;
	MyEvents[7].event_number = 0xF2;MyEvents[7].umask_value = 0x04;
	MyEvents[8].event_number = 0x08;MyEvents[8].umask_value = 0x01;
	MyEvents[9].event_number = 0x08;MyEvents[9].umask_value = 0x02;
	MyEvents[10].event_number = 0x08;MyEvents[10].umask_value = 0x04;
	MyEvents[11].event_number = 0x08;MyEvents[11].umask_value = 0x10;
	MyEvents[12].event_number = 0x05;MyEvents[12].umask_value = 0x01;
	MyEvents[13].event_number = 0x24;MyEvents[13].umask_value = 0x10;
	MyEvents[14].event_number = 0x24;MyEvents[14].umask_value = 0x20;
	MyEvents[15].event_number = 0x24;MyEvents[15].umask_value = 0x30;
	MyEvents[16].event_number = 0x24;MyEvents[16].umask_value = 0x40;
	MyEvents[17].event_number = 0x24;MyEvents[17].umask_value = 0x80;
	MyEvents[18].event_number = 0x24;MyEvents[18].umask_value = 0xC0;
	MyEvents[19].event_number = 0x2E;MyEvents[19].umask_value = 0x4F;
	MyEvents[20].event_number = 0x2E;MyEvents[20].umask_value = 0x41;
	MyEvents[21].event_number = 0x49;MyEvents[21].umask_value = 0x01;
	MyEvents[22].event_number = 0x49;MyEvents[22].umask_value = 0x02;
	MyEvents[23].event_number = 0x49;MyEvents[23].umask_value = 0x04;
	MyEvents[24].event_number = 0x49;MyEvents[24].umask_value = 0x10;

	uint64 BeforeTime = 0, AfterTime = 0;
	int curRowindex = 0;
	cout << "\n\nif m good\n\n";
	if (m->good())
		m->program(PCM::CUSTOM_CORE_EVENTS, &MyEvents);
	SystemCounterState stateBefore = getSystemCounterState();
	// Begin the query
	BeforeTime = m->getTickCount(1000000);

	for (int i = 0; i < numRows; i++) {
		if (db[AbaseIndex + i] <= selectivity) {
			for (int colId = 0; colId < numCols; colId++) {
				sum += db[(colId * numRows) + i];
			}
		}
	}
	// End of the query
	AfterTime = m->getTickCount(1000000);
	SystemCounterState stateAfter = getSystemCounterState();


	//Now, let's get CPU counters for the duration of the query

	uint64 L2_RQSTS_DEMAND_DATA_RD_HIT = getNumberOfCustomEvents(0, stateBefore,stateAfter);
	cout << "\n\nDemand Data Read requests that hit L2 cache.: " << std::dec<< L2_RQSTS_DEMAND_DATA_RD_HIT << endl;
	uint64 L2_RQSTS_ALL_DEMAND_DATA_RD = getNumberOfCustomEvents(1, stateBefore,stateAfter);
	cout << "\n\nCounts any demand and L1 HW prefetch data loadrequests to L2.: " << std::dec<< L2_RQSTS_ALL_DEMAND_DATA_RD << endl;
	uint64 L1D_REPLACEMENT = getNumberOfCustomEvents(2, stateBefore,stateAfter);
	cout << "\n\nCounts the number of lines brought into the L1 datacache.: " << std::dec<< L1D_REPLACEMENT << endl;
	uint64 L1D_EVICTION = getNumberOfCustomEvents(3, stateBefore,stateAfter);
	cout << "\n\nCounts the number of modified lines evicted fromthe L1 data cache due to replacement.: " << std::dec<< L1D_EVICTION << endl;
	uint64 L1D_ALL_M_REPLACEMENT = getNumberOfCustomEvents(4, stateBefore,stateAfter);
	cout << "\n\nCache lines in M state evicted out of L1D due toSnoop HitM or dirty line replacement.: " << std::dec<< L1D_ALL_M_REPLACEMENT << endl;
	uint64 L2_TRANS_DEMAND_DATA_RD = getNumberOfCustomEvents(5, stateBefore,stateAfter);
	cout << "\n\nDemand Data Read requests that access L2 cache: " << std::dec<< L2_TRANS_DEMAND_DATA_RD << endl;
	uint64 L2_LINES_OUT_DEMAND_CLEAN = getNumberOfCustomEvents(6, stateBefore,stateAfter);
	cout << "\n\nClean L2 cache lines evicted by demand.: " << std::dec<< L2_LINES_OUT_DEMAND_CLEAN << endl;
	uint64 L2_LINES_OUT_PF_CLEAN = getNumberOfCustomEvents(7, stateBefore,stateAfter);
	cout << "\n\nClean L2 cache lines evicted by L2 prefetch.: " << std::dec<< L2_LINES_OUT_PF_CLEAN << endl;
	uint64 DTLB_LOAD_MISSES_MISS_CAUSES_A_WALK = getNumberOfCustomEvents(8, stateBefore,stateAfter);
	cout << "\n\nMisses in all TLB levels that cause a page walk of: " << std::dec<< DTLB_LOAD_MISSES_MISS_CAUSES_A_WALK << endl;
	uint64 DTLB_LOAD_MISSES_WALK_COMPLETED = getNumberOfCustomEvents(9, stateBefore,stateAfter);
	cout << "\n\nMisses in all TLB levels that caused page walk completed of any size.: " << std::dec<< DTLB_LOAD_MISSES_WALK_COMPLETED << endl;
	uint64 DTLB_LOAD_MISSES_WALK_DURATION = getNumberOfCustomEvents(10, stateBefore,stateAfter);
	cout << "\n\nCycle PMH is busy with a walk.: " << std::dec<< DTLB_LOAD_MISSES_WALK_DURATION << endl;
	uint64 DTLB_LOAD_MISSES_STLB_HIT = getNumberOfCustomEvents(11, stateBefore,stateAfter);
	cout << "\n\nNumber of cache load STLB hits. No page walk.: " << std::dec<< DTLB_LOAD_MISSES_STLB_HIT << endl;
	uint64 MISALIGN_MEM_REF_LOADS = getNumberOfCustomEvents(12, stateBefore,stateAfter);
	cout << "\n\nSpeculative cache-line split load uops dispatched to L1D.: " << std::dec<< MISALIGN_MEM_REF_LOADS << endl;
	uint64 L2_RQSTS_CODE_RD_HIT = getNumberOfCustomEvents(13, stateBefore,stateAfter);
	cout << "\n\nNumber of instruction fetches that hit the L2 cache.: " << std::dec<< L2_RQSTS_CODE_RD_HIT << endl;
	uint64 L2_RQSTS_CODE_RD_MISS = getNumberOfCustomEvents(14, stateBefore,stateAfter);
	cout << "\n\nNumber of instruction fetches that missed the L2 cache.: " << std::dec<< L2_RQSTS_CODE_RD_MISS << endl;
	uint64 L2_RQSTS_ALL_CODE_RD = getNumberOfCustomEvents(15, stateBefore,stateAfter);
	cout << "\n\nCounts all L2 code requests.: " << std::dec<< L2_RQSTS_ALL_CODE_RD << endl;
	uint64 L2_RQSTS_PF_HIT = getNumberOfCustomEvents(16, stateBefore,stateAfter);
	cout << "\n\nRequests from L2 Hardware prefetcher that hit L2.: " << std::dec<< L2_RQSTS_PF_HIT << endl;
	uint64 L2_RQSTS_PF_MISS = getNumberOfCustomEvents(17, stateBefore,stateAfter);
	cout << "\n\nRequests from L2 Hardware prefetcher that missed L2.: " << std::dec<< L2_RQSTS_PF_MISS << endl;
	uint64 L2_RQSTS_ALL_PF = getNumberOfCustomEvents(18, stateBefore,stateAfter);
	cout << "\n\nAny requests from L2 Hardware prefetchers.: " << std::dec<< L2_RQSTS_ALL_PF << endl;
	uint64 LONGEST_LAT_CACHE_REFERENCE = getNumberOfCustomEvents(19, stateBefore,stateAfter);
	cout << "\n\nThis event counts requests originating from the core that reference a cache line in the last level cache.: " << std::dec<< LONGEST_LAT_CACHE_REFERENCE << endl;
	uint64 LONGEST_LAT_CACHE_MISS = getNumberOfCustomEvents(20, stateBefore,stateAfter);
	cout << "\n\nThis event counts each cache miss condition for references to the last level cache: " << std::dec<< LONGEST_LAT_CACHE_MISS << endl;
	uint64 DTLB_STORE_MISSES_MISS_CAUSES_A_WALK = getNumberOfCustomEvents(21, stateBefore,stateAfter);
	cout << "\n\nMiss in all TLB levels causes a page walk of any page size (4K/2M/4M/1G).: " << std::dec<< DTLB_STORE_MISSES_MISS_CAUSES_A_WALK << endl;
	uint64 DTLB_STORE_MISSES_WALK_COMPLETED = getNumberOfCustomEvents(22, stateBefore,stateAfter);
	cout << "\n\nMiss in all TLB levels causes a page walk that completes of any page size (4K/2M/4M/1G).: " << std::dec<< DTLB_STORE_MISSES_WALK_COMPLETED << endl;
	uint64 DTLB_STORE_MISSES_WALK_DURATION = getNumberOfCustomEvents(23, stateBefore,stateAfter);
	cout << "\n\nCycles PMH is busy with this walk.: " << std::dec<< DTLB_STORE_MISSES_WALK_DURATION << endl;
	uint64 DTLB_STORE_MISSES_STLB_HIT = getNumberOfCustomEvents(24, stateBefore,stateAfter);
	cout << "\n\nStore operations that miss the first TLB level but hit the second and do not cause page walks.: " << std::dec<< DTLB_STORE_MISSES_STLB_HIT << endl;




	cout << "getL2CacheHitRatio: "
			<< getL2CacheHitRatio(stateBefore, stateAfter) << endl;

	cout << "getL3CacheMisses(before_sstate, after_sstate): "
			<< getL3CacheMisses(stateBefore, stateAfter) << endl;

	cout << "getL3CacheHitRatio: "
			<< getL3CacheHitRatio(stateBefore, stateAfter) << endl;

	cout << "time (u sec): "<<  std::dec <<(AfterTime - BeforeTime)<< endl;

    ofstream outputFile;
    outputFile.open ("output.txt",ios::app);
    outputFile 	<< programVer<<"\t"
    			<< SELECT_SUM_B_WHERE_A <<"\t"
    			<<	numRows <<"\t"
    			<< numCols <<"\t"
    			<< A_col_id <<"\t"
    			<< selectivity<< "\t"
		        << (AfterTime - BeforeTime)<<"\t"
    			<< sum  <<"\t"
    			<< getL2CacheHits(stateBefore, stateAfter) << "\t"
    			<< getL2CacheMisses(stateBefore, stateAfter) << "\t"
    			<< getCyclesLostDueL2CacheMisses(stateBefore, stateAfter) << "\t"
    			<< getL2CacheHitRatio(stateBefore, stateAfter)<<"\t"
    			<< getL3CacheHits(stateBefore, stateAfter) << "\t"
    			<< getL3CacheMisses(stateBefore, stateAfter) << "\t"
    			<< getL3CacheHitRatio(stateBefore, stateAfter) << "\t"
    			<< getCyclesLostDueL3CacheMisses(stateBefore, stateAfter) << "\t"
    			<< getL3CacheHitsNoSnoop(stateBefore, stateAfter) << "\t"
    			<< getL3CacheHitsSnoop(stateBefore, stateAfter) << "\t"
    			<< std::dec
    			<< L2_RQSTS_DEMAND_DATA_RD_HIT<< "\t"
    			<< L2_RQSTS_ALL_DEMAND_DATA_RD<< "\t"
    			<< L1D_REPLACEMENT<< "\t"
    			<< L1D_EVICTION<< "\t"
    			<< L1D_ALL_M_REPLACEMENT<< "\t"
    			<< L2_TRANS_DEMAND_DATA_RD<< "\t"
    			<< L2_LINES_OUT_DEMAND_CLEAN<< "\t"
    			<< L2_LINES_OUT_PF_CLEAN<< "\t"
    			<< DTLB_LOAD_MISSES_MISS_CAUSES_A_WALK<< "\t"
    			<< DTLB_LOAD_MISSES_WALK_COMPLETED<< "\t"
    			<< DTLB_LOAD_MISSES_WALK_DURATION<< "\t"
    			<< DTLB_LOAD_MISSES_STLB_HIT<< "\t"
    			<< MISALIGN_MEM_REF_LOADS<< "\t"
    			<< L2_RQSTS_CODE_RD_HIT<< "\t"
    			<< L2_RQSTS_CODE_RD_MISS<< "\t"
    			<< L2_RQSTS_ALL_CODE_RD<< "\t"
    			<< L2_RQSTS_PF_HIT<< "\t"
    			<< L2_RQSTS_PF_MISS<< "\t"
    			<< L2_RQSTS_ALL_PF<< "\t"
    			<< LONGEST_LAT_CACHE_REFERENCE<< "\t"
    			<< LONGEST_LAT_CACHE_MISS<< "\t"
    			<< DTLB_STORE_MISSES_MISS_CAUSES_A_WALK<< "\t"
    			<< DTLB_STORE_MISSES_WALK_COMPLETED<< "\t"
    			<< DTLB_STORE_MISSES_WALK_DURATION<< "\t"
    			<< DTLB_STORE_MISSES_STLB_HIT<< "\t"

			<<endl;
    outputFile.close();


	delete[] db;
	m->cleanup();

}

