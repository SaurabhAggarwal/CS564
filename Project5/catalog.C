#include "catalog.h"
#include "stdio.h"

RelCatalog::RelCatalog(Status &status) :
	 HeapFile(RELCATNAME, status)
{
// nothing should be needed here
}

/**
 * FUNCTION: RelCatalog::getInfo
 *
 * PURPOSE:  To get information about a particular relation.
 *
 * PARAMETERS:
 *		relation 	(in)		Relation name whose information is to be returned
 *		record		(out)		Record object corresponding to the information about the relation
 *	
 * RETURN VALUES:
 * 		Status	OK 				Relation information successfully found and returned.
 *				BADCATPARM 		Relation name is empty
 *				BADSCANPARM  	Error in allocating page: All buffer frames are pinned
 *				FILEEOF 		Reached the end of file while scanning for the record
 *				BUFFEREXCEEDED  All buffer frames are pinned
 *				HASHTBLERROR	Hash table error occurred
 *				PAGENOTPINNED 	Pin count is already 0
 *				HASHNOTFOUND  	Page is not in the buffer pool hash table
 **/
const Status RelCatalog::getInfo(const string & relation, RelDesc &record)
{
  	if (relation.empty())
    	return BADCATPARM;

  	Status status;
  	Record rec;
  	RID rid;

	HeapFileScan* hfs = new HeapFileScan(RELCATNAME, status);
	if (status != OK)
	{
		delete hfs;
		return status;
	}
	if ((status = hfs->startScan(0, 32, STRING, relation.c_str(), EQ)) != OK) return status;

	while ((status = hfs->scanNext(rid)) != FILEEOF)
  	{
 		if (status == OK)
    	{

	    	status = hfs->getRecord(rec);
			if (status != OK) break;
          
  	  		memcpy(&record, rec.data, rec.length);
  	  		break;
  		}
	}

	// Close scan and data file
  	hfs->endScan();
  	
  	delete hfs;
	
	if(status != OK) status = RELNOTFOUND;
	return status;
}

/**
 * FUNCTION: RelCatalog::addInfo
 *
 * PURPOSE:  To add information about a new relation created.
 *
 * PARAMETERS:
 *		record 		(in)		Record information to be added.
 *	
 * RETURN VALUES:
 * 		Status	OK 				Record successfully added.
 *				UNIXERR 		Error in allocating page: Unix error occurred
 *				BUFFEREXCEEDED  Error in allocating page: All buffer frames are pinned
 *				HASHTBLERROR	Error in allocating page: Hash table error occurred
 *				PAGENOTPINNED 	Error in unpinning page: Pin count is already 0
 *				HASHNOTFOUND  	Error in unpinning page: Page is not in the buffer pool hash table
 **/
const Status RelCatalog::addInfo(RelDesc & record)
{
  RID rid;
  Status status;

	// First, create an InsertFileScan object on the relation catalog table.
	InsertFileScan*  ifs = new InsertFileScan(RELCATNAME, status);
	if (status != OK) return status;
	
	// Next, create a record and then insert it into the relation catalog table using the method insertRecord of InsertFileScan.
	Record rec;
	rec.data = &record;
	rec.length = sizeof(RelDesc);
	if((status = ifs->insertRecord(rec, rid)) != OK) return status;
	
	delete ifs;
	return status;
}

/**
 * FUNCTION: RelCatalog::removeInfo
 *
 * PURPOSE:  To remove information about a relation from relation catalog.
 *
 * PARAMETERS:
 *		relation 	(in)		Name of the relation whose information is to be removed.
 *	
 * RETURN VALUES:
 * 		Status	OK 				Relation information successfully removed
 *				BADCATPARM 		Relation name is empty
 *				UNIXERR 		Error in allocating page: Unix error occurred
 *				BADSCANPARM  	Error in allocating page: All buffer frames are pinned
 *				FILEEOF 		Reached the end of file while scanning for the record
 *				HASHTBLERROR	Hash table error occurred
 *				PAGENOTPINNED 	Pin count is already 0
 *				HASHNOTFOUND  	Page is not in the buffer pool hash table
 **/
const Status RelCatalog::removeInfo(const string & relation)
{
	Status status;
  	RID rid;

  	if(relation.empty()) return BADCATPARM;

	// Remove the tuple corresponding to relName from relcat. Once again, you have to start a filter scan on relcat to locate the rid of the desired tuple. Then you can call deleteRecord() to remove it.
	HeapFileScan* hfs = new HeapFileScan(RELCATNAME, status);
	if (status != OK) return status;

  	if((status = hfs->startScan(0, 32, STRING, relation.c_str(), EQ)) != OK) return status;
  
  	Status tempStatus = UNIXERR;
	while ((status = hfs->scanNext(rid)) != FILEEOF)
  	{
		if (status == OK)
    	{
    		tempStatus = hfs->deleteRecord();
      		break;
    	}
	}
	
	if(tempStatus == OK) status = OK;

	// Close scan and data file
  	hfs->endScan();
  	
  	delete hfs;
	
	return status;
}


RelCatalog::~RelCatalog()
{
// nothing should be needed here
}


AttrCatalog::AttrCatalog(Status &status) :
	 HeapFile(ATTRCATNAME, status)
{
// nothing should be needed here
}


/**
 * FUNCTION: AttrCatalog::getInfo
 *
 * PURPOSE:  To get information about a particular attribute of a particular relation.
 *
 * PARAMETERS:
 *		relation 	(in)	Name of the relation
 *		attrName	(in)	Name of the attribute in the relation whose information is to be returned
 *		record		(out)	Information about the attribute of the relation
 *	
 * RETURN VALUES:
 * 		Status	OK 				Attribute information successfully found and returned
 *				BADCATPARM 		Relation name is empty
 *				RELNOTFOUND		Relation was not found
 *				UNIXERR 		Error in allocating page: Unix error occurred
 *				FILEEOF 		Reached the end of file while scanning for the record
 *				HASHTBLERROR	Hash table error occurred
 *				PAGENOTPINNED 	Pin count is already 0
 *				HASHNOTFOUND  	Page is not in the buffer pool hash table
 **/
const Status AttrCatalog::getInfo(const string & relation, 
				  const string & attrName,
				  AttrDesc &record)
{

  Status status;
  RID rid;
  Record rec;

  if (relation.empty() || attrName.empty()) return BADCATPARM;

  	HeapFileScan*  hfs = new HeapFileScan(ATTRCATNAME, status);
	if (status != OK) return status;

	if ((status = hfs->startScan(0, 32, STRING, relation.c_str(), EQ)) != OK) return status;
	
	while ((status = hfs->scanNext(rid)) != FILEEOF)
  	{
		if (status == OK)
    		{
	    		status = hfs->getRecord(rec);
			if (status != OK) break;
          
		  	memcpy(&record, rec.data, rec.length);
  			if(strcmp(record.relName, relation.c_str()) == 0 && 
			   strcmp(record.attrName, attrName.c_str()) == 0)
				break;
		}
	}
	
	//Close scan and data file
  	if((status = hfs->endScan()) != OK)
    	return status;
  	delete hfs;
	
	return status;
}


/**
 * FUNCTION: AttrCatalog::addInfo
 *
 * PURPOSE:  To add information about a particular attribute of a particular relation.
 *
 * PARAMETERS:
 *		record 		(in)		Attribute description of a particular attribute of a relation.
 *	
 * RETURN VALUES:
 * 		Status	OK 				Attribute information successfully added
 *				UNIXERR 		Error in allocating page: Unix error occurred
 *				BUFFEREXCEEDED  Error in allocating page: All buffer frames are pinned
 *				HASHTBLERROR	Error in allocating page: Hash table error occurred
 *				PAGENOTPINNED 	Error in unpinning page: Pin count is already 0
 *				HASHNOTFOUND  	Error in unpinning page: Page is not in the buffer pool hash table
 **/
const Status AttrCatalog::addInfo(AttrDesc & record)
{
  RID rid;
  Status status;

  // Adds a tuple (corresponding to an attribute of a relation) to the attrcat relation.

	// First, create an InsertFileScan object on the relation catalog table.
	InsertFileScan*  ifs = new InsertFileScan(ATTRCATNAME, status);
	if (status != OK) return status;
	
	// Next, create a record and then insert it into the relation catalog table using the method insertRecord of InsertFileScan.
	Record rec;
	rec.data = &record;
	rec.length = sizeof(AttrDesc);
	status = ifs->insertRecord(rec, rid);
	
	delete ifs;
	
	return status;
}


/**
 * FUNCTION: AttrCatalog::removeInfo
 *
 * PURPOSE:  To remove information about a particular attribute of a particular relation.
 *
 * PARAMETERS:
 *		relation 	(in)	Name of the relation whose attribute's information is to be removed
 *		attrName	(in)	Name of the attribute whose information is to be removed
 *	
 * RETURN VALUES:
 * 		Status	OK 				Attribute information successfully removed
 *				FILEEOF 		Reached the end of file while scanning for the record
 *				HASHTBLERROR	Hash table error occurred
 *				PAGENOTPINNED 	Pin count is already 0
 *				HASHNOTFOUND  	Page is not in the buffer pool hash table
 **/
const Status AttrCatalog::removeInfo(const string & relation, 
			       const string & attrName)
{
  Status status;
  Record rec;
  RID rid;
  AttrDesc record;

  if (relation.empty() || attrName.empty()) return BADCATPARM;

	// Removes the tuple from attrcat that corresponds to attribute attrName of relation.
  	HeapFileScan*  hfs = new HeapFileScan(ATTRCATNAME, status);
	if (status != OK) return status;

	if ((status = hfs->startScan(0, 32, STRING, relation.c_str(), EQ)) != OK) return status;
	
	Status	tempStatus = UNIXERR;
	while ((status = hfs->scanNext(rid)) != FILEEOF)
	{
		if (status == OK)
		{
  		status = hfs->getRecord(rec);
			if (status != OK) break;
        
	  	memcpy(&record, rec.data, rec.length);
			if(strcmp(record.relName, relation.c_str()) == 0 && 
		   	strcmp(record.attrName, attrName.c_str()) == 0)
			{ 
		      tempStatus = hfs->deleteRecord();
		      break;
			}
		}
	}
	
	if(tempStatus == OK) status = OK;
	
  	status = hfs->endScan();
	
	delete hfs;
	
	return status;
}


/**
 * FUNCTION: AttrCatalog::getRelInfo
 *
 * PURPOSE:  Returns (by reference) descriptors for all attributes of the relation.
 *
 * PARAMETERS:
 *		relation	(in)		Name of the relation whose descriptors are to be returned
 *		attrCnt		(in)		Number of attributes in the relation
 *		attrs 		(out)		File name for the heap file
 *	
 * RETURN VALUES:
 * 		Status	OK 				Descriptors of all attributes successfully returned.
 *				BADCATPARM 		Relation name is empty
 *				RELNOTFOUND		Relation was not found
 *				UNIXERR 		Error in allocating page: Unix error occurred
 *				FILEEOF 		Reached the end of file while scanning for the record
 *				HASHTBLERROR	Hash table error occurred
 *				PAGENOTPINNED 	Pin count is already 0
 *				HASHNOTFOUND  	Page is not in the buffer pool hash table
 **/
const Status AttrCatalog::getRelInfo(const string & relation, 
				     int &attrCnt,
				     AttrDesc *&attrs)
{
  Status status;
  RID rid;
  Record rec;
  AttrDesc tempRec;

  if (relation.empty()) return BADCATPARM;
	attrCnt = 0;
	
	RelDesc relRec;
	HeapFileScan*  hfs = new HeapFileScan(RELCATNAME, status);
	if (status != OK) return status;
	if ((status = hfs->startScan(0, 32, STRING, relation.c_str(), EQ)) != OK) return status;
		while ((status = hfs->scanNext(rid)) != FILEEOF)
	{
		if (status == OK)
		{
			status = hfs->getRecord(rec);
			if (status != OK) 
			{ delete hfs; return status;}
			
			memcpy(&relRec, rec.data, rec.length);
			attrCnt = relRec.attrCnt;
			break;
		}
	}
	attrs = new AttrDesc[attrCnt];

	HeapFileScan*  newhfs = new HeapFileScan(ATTRCATNAME, status);
	
	// Scan again
	int i = 0;
	if ((status = newhfs->startScan(0, 32, STRING, relation.c_str(), EQ)) != OK) return status;
	
	Status tempStatus = UNIXERR;
	while ((status = newhfs->scanNext(rid)) != FILEEOF)
	{
		if (status == OK)
		{
		tempStatus = newhfs->getRecord(rec);
			if (tempStatus != OK) break;
        memcpy(&tempRec, rec.data, rec.length);
			if(strcmp(tempRec.relName, relation.c_str()) == 0)
	  	{
				memcpy(&attrs[i], rec.data, rec.length);
				i++;
			}
		}
	}
	
	if(tempStatus == OK) status = OK;

	// Close scan and data file
	hfs->endScan();
	newhfs->endScan();
  	
  	delete hfs;
  	delete newhfs;
	
	if(status != OK)
		status = RELNOTFOUND;
	
	return status;
}


AttrCatalog::~AttrCatalog()
{
// nothing should be needed here
}
