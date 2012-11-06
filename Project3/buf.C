#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{
	unsigned int initialClockHand = clockHand;
	
	int totalPinnedPages = 0;
	
	int round = 0;
	do
	{
		if(clockHand == initialClockHand)
		{
			round++;
			totalPinnedPages = 0;
		}
		
		advanceClock();
			
		BufDesc *currentBufDesc = &bufTable[clockHand];
		
		if(!currentBufDesc -> valid)
		{
			frame = currentBufDesc -> frameNo;
			return OK;
		}
		
		if(currentBufDesc -> refbit)
		{
			currentBufDesc -> refbit = false;
			if(currentBufDesc -> pinCnt > 0)	//Saving extra cycle of Clock Algorithm
				totalPinnedPages++;
			continue;
		}
		else
		{
			if(currentBufDesc -> pinCnt > 0)
			{
				totalPinnedPages++;
				continue;
			}
			else
			{
				if(currentBufDesc -> dirty)
				{
					Status returnStatus = currentBufDesc -> file -> writePage(currentBufDesc -> pageNo, &(bufPool[clockHand]));
					if(returnStatus != OK)
						return UNIXERR;
				}
				hashTable -> remove(currentBufDesc -> file, currentBufDesc -> pageNo); //Removed from hash on frame eviction.
				frame = currentBufDesc -> frameNo;
				return OK;
			}
		}
		
	} while(((clockHand != initialClockHand) || (round <= 2)) && (totalPinnedPages != numBufs));
	
	return BUFFEREXCEEDED;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
	int frameNo = 0;
	Status returnStatus = hashTable -> lookup(file, PageNo, frameNo);
	if(returnStatus == OK)
	{
		bufTable[frameNo].pinCnt++;
		bufTable[frameNo].refbit = true;
		page = &bufPool[frameNo];
	}
	else //returnStatus = HASHNOTFOUND
	{
		returnStatus = allocBuf(frameNo);
		if(returnStatus == OK)
		{
			returnStatus = file -> readPage(PageNo, &bufPool[frameNo]);
			if(returnStatus == OK)
			{
				returnStatus = hashTable -> insert(file, PageNo, frameNo);
				if(returnStatus == OK)
				{
					bufTable[frameNo].Set(file, PageNo);
					page = &bufPool[frameNo];
				}
			}
		}
	}
	
	return returnStatus;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
	int frameNo = 0;
	Status returnStatus = hashTable -> lookup(file, PageNo, frameNo);
	if(returnStatus == OK)
	{
		if(bufTable[frameNo].pinCnt == 0)
			returnStatus = PAGENOTPINNED;
		
		if(returnStatus == OK)
		{
			bufTable[frameNo].pinCnt--;
		
			if(dirty == true)
				bufTable[frameNo].dirty = true;
		}
	}
	
	return returnStatus;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
	//allocate an empty page using file -> allocatePage()
	Status returnStatus = file -> allocatePage(pageNo);
	if(returnStatus == OK)
	{
		int frameNo = 0;
		returnStatus = BufMgr::allocBuf(frameNo);
		if(returnStatus == OK)
		{
			returnStatus = hashTable -> insert(file, pageNo, frameNo);
			if(returnStatus == OK)
			{
				bufTable[frameNo].Set(file, pageNo);
				page = &bufPool[frameNo];
			}
		}
	}
	
	return returnStatus;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


