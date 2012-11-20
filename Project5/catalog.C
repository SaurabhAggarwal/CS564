#include "catalog.h"
#include "stdio.h"

RelCatalog::RelCatalog(Status &status) :
	 HeapFile(RELCATNAME, status)
{
// nothing should be needed here
}


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

	//while ((status = ((HeapFileScan*)this)->scanNext(rid)) != FILEEOF)
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
  	//if ((status = hfs->endScan()) != OK)
  	//  return status;
 	delete hfs;
	
	if(status != OK) status = RELNOTFOUND;
	return status;
}


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
  	//if ((status = hfs->endScan()) != OK)
  	//  return status;
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
	
	// Close scan and data file
  if ((status = hfs->endScan()) != OK)
    return status;
  delete hfs;

	return status;
}


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
	
  // Close scan and data file
  status = hfs->endScan();
//  if ((status = hfs->endScan()) != OK)
//    return status;
  delete hfs;


	return status;
}


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
			//if(strcmp(relRec.relName, relation.c_str()) == 0)
			//{
				attrCnt = relRec.attrCnt;
				break;
			//}
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
//  if ((status = hfs->endScan()) != OK)
//    return status;
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


