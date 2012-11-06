#include "heapfile.h"
#include "error.h"

/**
 * FUNCTION: createHeapFile
 *
 * PURPOSE:  To create an empty heap file (with header page and one data page with no records).
 *
 * PARAMETERS:
 *		filename 	(in)	File name for the heap file
 *	
 * RETURN VALUES:
 * 		Status	OK 			Heap File created successfully
 *				UNIXERR 		Error in allocating page: Unix error occurred
 *				BUFFEREXCEEDED  Error in allocating page: All buffer frames are pinned
 *				HASHTBLERROR	Error in allocating page: Hash table error occurred
 *				PAGENOTPINNED 	Error in unpinning page: Pin count is already 0
 *				HASHNOTFOUND  	Error in unpinning page: Page is not in the buffer pool hash table
 **/
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
		
		status = db.createFile(fileName);
		if(status == OK)
		{
			status = db.openFile(fileName, file);
			if(status != OK)
				return status;
	
			// Allocate a page using Buffer Manager
			Page *actualHdrPage; 
			status = bufMgr->allocPage(file, hdrPageNo, actualHdrPage);
			
			if(status != OK)
				return status;
			
			
			status = bufMgr->allocPage(file, newPageNo, newPage);
			if(status != OK)
				return status;
				
			newPage->init(newPageNo);			
	
  			// Initialize the HeaderPage Structure now
			hdrPage = (FileHdrPage*) actualHdrPage; 
			strcpy(hdrPage->fileName, fileName.c_str());			
			hdrPage->recCnt = 0;			
			hdrPage->pageCnt = 1;
			hdrPage->firstPage = newPageNo;
			hdrPage->lastPage = newPageNo;
			
			// Marking both pages as dirty and unpining them.
			status = bufMgr->unPinPage(file, hdrPageNo, true);
			if(status != OK)
				return status;
			status = bufMgr->unPinPage(file, newPageNo, true);
			if(status != OK)
				return status;
			
			status = db.closeFile(file);
			
			return status;
		}	
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

/**
 * FUNCTION: HeapFile (Constructor)
 *
 * PURPOSE:  To initialize HeapFile instance and open the underlying file.
 *
 * PARAMETERS:
 *		filename 	(in)	File name for the heap file
 *
 * 		returnStatus	(out)	Status to be returned
 *				OK 				Underlying file was opened and the heap file initialized successfully
 *				UNIXERR 		Unix error occurred while opening the file or reading a page
 *				BUFFEREXCEEDED  All buffer frames are pinned
 *				HASHTBLERROR	Hash table error occurred
 *				PAGENOTPINNED 	Pin count is already 0
 *				HASHNOTFOUND  	Page is not in the buffer pool hash table
 **/
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((returnStatus = db.openFile(fileName, filePtr)) == OK)
    {
	    // Setting headerPageNo
		returnStatus = filePtr->getFirstPage(headerPageNo);
		if(returnStatus != OK)
			return;
		
		// Setting hdrDirtyFlag
		hdrDirtyFlag = false;
		
		// Read the Header Page 
		if((returnStatus = bufMgr->readPage(filePtr, headerPageNo, pagePtr)) != OK)
			return;
		
		// Setting FileHdrPage
		headerPage = (FileHdrPage*) pagePtr;	
    		
		// Read the First Data page (next to Header Page) and set it as current page
		curPageNo = headerPage->firstPage;
		if((returnStatus = bufMgr->readPage(filePtr, curPageNo, curPage)) != OK)
			return;
		
		// Setting curDirtyFlag and curRec
		curDirtyFlag = false;
		curRec = NULLRID;
	}
    else
    {
    	cerr << "open of heap file failed\n";
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

/**
 * FUNCTION: getRecord
 *
 * PURPOSE:  To retrieve a record (via the rec structure) given the RID of the record.
 *
 * PARAMETERS:
 *		rid 	(in)	Record ID of the record we want to retrieve
 *		rec		(in)	Record structure
 *
 * RETURN VALUES:
 * 		status	OK 				Record was retrieved successfully
 *				UNIXERR 		Unix error occurred while reading a page
 *				BUFFEREXCEEDED  All buffer frames are pinned
 *				HASHTBLERROR	Hash table error occurred
 *				PAGENOTPINNED 	Pin count is already 0
 *				HASHNOTFOUND  	Page is not in the buffer pool hash table
 **/
const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
	// Retrieve an arbitrary record from a file.
	// If record is not on the currently pinned page, the current page
	// is unpinned and the required page is read into the buffer pool
	// and pinned.  returns a pointer to the record via the rec parameter

    Status status;	
	
    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    
    // If the current page is not the page we want
	if(rid.pageNo != curPageNo)
	{
		// Unpin the current page so that it can be flushed to disk
		status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		if(status != OK)
			return status;
		curDirtyFlag = false;
		curPage = NULL;
	
		// Read the required page in buffer pool	
		status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
		if(status != OK)
			return status;
		
		// Update the current page number
		curPageNo = rid.pageNo;
	}
	
	// Retrieve record
	status = curPage->getRecord(rid, rec);
	if(status != OK)
		return status;
	
	// Set curRec
	curRec = rid;
	
	return status;
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}

/**
 * FUNCTION: HeapFileScan::scanNext
 *
 * PURPOSE:  To find and return the next record that matches the filter.
 *
 * PARAMETERS:
 *		outRid 	(out)	Record ID of the next matching record
 *
 * RETURN VALUES:
 * 		status	OK 				Next matching record was successfully returned
 *				FILEEOF 		Reached the end of file while scanning for the record
 *				BUFFEREXCEEDED  All buffer frames are pinned
 *				HASHTBLERROR	Hash table error occurred
 *				PAGENOTPINNED 	Pin count is already 0
 *				HASHNOTFOUND  	Page is not in the buffer pool hash table
 **/

const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		tmpRid;
    Record      rec;

    //For each page starting from current page, scan each record (continuing from curRec). If record matches search criteria, set curRec to it, and return it via outRid.
    do
    {
    	if((curRec.pageNo == -1) && (curRec.slotNo == -1))
    	{
    		status = curPage->firstRecord(tmpRid);
    		if(status != OK)
    			status = ENDOFPAGE;
    	}
    	else
	    {
	    	status = curPage->nextRecord(curRec, tmpRid);
    	}
    	
    	while(status == ENDOFPAGE)	//Reached the last record on that page, move onto next page.
    	{
    		//Was already on last page, so no more records found.
    		if(headerPage->lastPage == curPageNo)
    			return FILEEOF;
    		
    		//Was not on last page, unpin this page and get the next page.
    		status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if((status != OK) && (status != PAGENOTPINNED))
				return status;
    		
    		curDirtyFlag = false;
    		
    		//Getting next page.
    		status = curPage->getNextPage(curPageNo);
    		if(status != OK)
    			return status;
    		
    		status = bufMgr->readPage(filePtr, curPageNo, curPage);
    		if(status != OK)
    			return status;
    		
    		//Set tmpRid to be the first record of the new page.
    		status = curPage->firstRecord(tmpRid);
    		if(status != OK)
    			status = ENDOFPAGE;
    	}
    	curRec = tmpRid;
    	status = getRecord(rec);
	    if(status != OK)
    		return status;
    } while(!matchRec(rec));
    
	outRid = curRec;
	
	return OK;
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

/**
 * FUNCTION: InsertFileScan::insertRecord
 *
 * PURPOSE:  To insert the given record into the file and return the RID in the outRid parameter.
 *
 * PARAMETERS:
 *		rec		(in)	Record structure
 *		outRid 	(out)	Record ID of the inserted record
 *
 * RETURN VALUES:
 * 		status	OK 				Record was retrieved inserted
 *				INVALIDRECLEN 	Record length was larger than page capacity
 *				BUFFEREXCEEDED  All buffer frames are pinned
 *				HASHTBLERROR	Hash table error occurred
 *				PAGENOTPINNED 	Pin count is already 0
 *				HASHNOTFOUND  	Page is not in the buffer pool hash table
 *				NOSPACE		  	No space to insert record, even on a new page
 **/
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Status	status, unpinstatus;
    Page *newPage;
    int newPageNo;
    
    //Check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        //Will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }
	
	//Make last page as current page
	unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
	if((unpinstatus != OK) && (unpinstatus != PAGENOTPINNED))
	{
		return unpinstatus;
	}
	curPage = NULL;
	curPageNo = 0;
	curDirtyFlag = false;
	status = bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
	if(status != OK)
		return status;
	
	curPageNo = headerPage->lastPage;
	
	//Attempt to insert record.
	status = curPage->insertRecord(rec, outRid);
	
	if(status == NOSPACE)
	{
		//If insufficient, allocate new page, make that current page, update FileHdrPage (last, PageCnt), make it current page.
		status = bufMgr->allocPage(filePtr, newPageNo, newPage);
		newPage->init(newPageNo);
		if(status != OK)
		{
			return status;
		}
		
		curPage->setNextPage(newPageNo);
		status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		if (status != OK)
			return status;
		curDirtyFlag = false;
		curPage = newPage;
		curPageNo = newPageNo;
		status = curPage->insertRecord(rec, outRid);
		if(status != OK)
			return status;
		
		headerPage->pageCnt += 1;
		headerPage->lastPage = curPageNo;
		hdrDirtyFlag = true;
	}
	
	curDirtyFlag = true;
	
	headerPage->recCnt += 1;
	curRec = outRid;
	
	return status;  
}
