#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "joinHT.h"
#include "stdio.h"
#include "stdlib.h"

extern JoinType JoinMethod;

const int matchRec(const Record & outerRec,
		   const Record & innerRec,
		   const AttrDesc & attrDesc1,
		   const AttrDesc & attrDesc2);

/*
 * Joins two relations.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

// implementation of nested loops join goes here
const Status QU_NL_Join(const string & result, 
		     const int projCnt, 
		     const attrInfo projNames[],
		     const attrInfo *attr1, 
		     const Operator op, 
		     const attrInfo *attr2)
{
    Status status;
    int resultTupCnt = 0;

    if (attr1->attrType != attr2->attrType ||
        attr1->attrLen != attr2->attrLen)
    {
        return ATTRTYPEMISMATCH;
    }
    
    
    // go through the projection list and look up each in the 
    // attr cat to get an AttrDesc structure (for offset, length, etc)
    AttrDesc attrDescArray[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        Status status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         attrDescArray[i]);
        if (status != OK)
        {
            return status;
        }
    }
    
    // get AttrDesc structure for the first join attribute
    AttrDesc attrDesc1;
    status = attrCat->getInfo(attr1->relName,
                                     attr1->attrName,
                                     attrDesc1);
    if (status != OK)
    {
        return status;
    }
    // get AttrDesc structure for the first join attribute
    AttrDesc attrDesc2;
    status = attrCat->getInfo(attr2->relName,
                              attr2->attrName,
                              attrDesc2);
    if (status != OK)
    {
        return status;
    }

    // get output record length from attrdesc structures
    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += attrDescArray[i].attrLen;
    }
    
    // open the result table
    InsertFileScan resultRel(result, status);
    if (status != OK) { return status; }

    char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;

    // start scan on outer table
    HeapFileScan outerScan(string(attrDesc1.relName), status);
    if (status != OK) { return status; }
    status = outerScan.startScan(0,
                                 0,
                                 STRING,
                                 NULL,
                                 EQ);
    if (status != OK) { return status; }
    
    // scan outer table
    RID outerRID;
    Record outerRec;
    
    Operator myop;
    switch(op) {
      case EQ:   myop=EQ; break;
      case GT:   myop=LT; break;
      case GTE:  myop=LTE; break;
      case LT:   myop=GT; break;
      case LTE:  myop=GTE; break;
      case NE:   myop=NE; break;
    }

    while (outerScan.scanNext(outerRID) == OK)
    {
        status = outerScan.getRecord(outerRec);
        ASSERT(status == OK);

        // scan inner table
        HeapFileScan innerScan(string(attrDesc2.relName), status);
        if (status != OK) { return status; }
        status = innerScan.startScan(attrDesc2.attrOffset,
                                     attrDesc2.attrLen,
                                     (Datatype) attrDesc2.attrType,
                                     ((char *)outerRec.data) + attrDesc1.attrOffset,
                                     myop);
        if (status != OK) { return status; }

        RID innerRID;
        while (innerScan.scanNext(innerRID) == OK)
        {
            Record innerRec;
            status = innerScan.getRecord(innerRec);
            ASSERT(status == OK);
            
            // we have a match, copy data into the output record
            int outputOffset = 0;
            for (int i = 0; i < projCnt; i++)
            {
                // copy the data out of the proper input file (inner vs. outer)
                if (0 == strcmp(attrDescArray[i].relName, attrDesc1.relName))
                {
                    memcpy(outputData + outputOffset,
                           (char *)outerRec.data + attrDescArray[i].attrOffset,
                           attrDescArray[i].attrLen);
                }
                else // get data from the inner record
                {
                    memcpy(outputData + outputOffset,
                           (char *)innerRec.data + attrDescArray[i].attrOffset,
                           attrDescArray[i].attrLen);                    
                }
                outputOffset += attrDescArray[i].attrLen;
            } // end copy attrs

            // add the new record to the output relation
            RID outRID;
            status = resultRel.insertRecord(outputRec, outRID);
            ASSERT(status == OK);
            resultTupCnt++;
        } // end scan inner
    } // end scan outer
    printf("tuple nested join produced %d result tuples \n", resultTupCnt);
    return OK;
}

// implementation of sort merge join goes here
const Status QU_SM_Join(const string & result, 
		     const int projCnt, 
		     const attrInfo projNames[],
		     const attrInfo *attr1, 
		     const Operator op, 
		     const attrInfo *attr2)
{
    Status status;

    if (attr1->attrType != attr2->attrType ||
        attr1->attrLen != attr2->attrLen)
    {
        return ATTRTYPEMISMATCH;
    }
    
    // go through the projection list and look up each in the 
    // attr cat to get an AttrDesc structure (for offset, length, etc)
    AttrDesc attrDescArray[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        Status status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         attrDescArray[i]);
        if (status != OK)
        {
            return status;
        }
    }
    
    // get AttrDesc structure for the first join attribute
    AttrDesc attrDesc1;
    status = attrCat->getInfo(attr1->relName,
                                     attr1->attrName,
                                     attrDesc1);
    if (status != OK)
    {
        return status;
    }
    // get AttrDesc structure for the first join attribute
    AttrDesc attrDesc2;
    status = attrCat->getInfo(attr2->relName,
                              attr2->attrName,
                              attrDesc2);
    if (status != OK)
    {
        return status;
    }

    // get output record length from attrdesc structures
    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += attrDescArray[i].attrLen;
    }


    // open sorted scans on both input files
    SortedFile sorted1(attrDesc1.relName,
                       attrDesc1.attrOffset,
                       attrDesc1.attrLen,
                       (Datatype) attrDesc1.attrType,
                       1000,
                       status);
    if (status != OK) { return status; }

    SortedFile sorted2(attrDesc2.relName,
                       attrDesc2.attrOffset,
                       attrDesc2.attrLen,
                       (Datatype) attrDesc2.attrType,
                       1000,
                       status);
    if (status != OK) { return status; }
    sorted2.setMark();

    // prepare output buffer
    char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;

    // open output relation file
    InsertFileScan resultRel(result, status);
    if (status != OK) { return status; }

    // scan the outer file
    bool firstTime = true;
    bool endOfInner = false;
    Record outerRec;
    while (sorted1.next(outerRec) == OK)
    {
        if (!firstTime)
        {
            // go back 
            sorted2.gotoMark();
            endOfInner = false;
        }
        else
            firstTime = false;
        
        // go forward until we get a match
        Record innerRec;
        bool done = false;
        while (!done)
        {
            if (OK != sorted2.next(innerRec))
            {
                endOfInner = true;
                break;
            }

            if (matchRec(outerRec, innerRec, attrDesc1, attrDesc2) <= 0)
                done = true;
        }
        sorted2.setMark();
        
        while (! endOfInner &&
               matchRec(outerRec, innerRec, attrDesc1, attrDesc2) == 0)
        {
            // we have a match, copy data into the output record
            int outputOffset = 0;
            for (int i = 0; i < projCnt; i++)
            {
                // copy the data out of the proper input file (inner vs. outer)
                if (0 == strcmp(attrDescArray[i].relName, attrDesc1.relName))
                {
                    memcpy(outputData + outputOffset,
                           (char *)outerRec.data + attrDescArray[i].attrOffset,
                           attrDescArray[i].attrLen);
                }
                else // get data from the inner record
                {
                    memcpy(outputData + outputOffset,
                           (char *)innerRec.data + attrDescArray[i].attrOffset,
                           attrDescArray[i].attrLen);                    
                }
                outputOffset += attrDescArray[i].attrLen;
            } // end copy attrs

            // add the new record to the output relation
            RID outRID;
            status = resultRel.insertRecord(outputRec, outRID);
            ASSERT(status == OK);

            // scan to the next entry in the inner sorted file
            if (OK != sorted2.next(innerRec))
                endOfInner = true;
        } // end scan inner
    } // end scan outer

    return OK;
}

// This is really not a hash join implementation.  It is actually a block nested
// loops join that uses hashing on each block of outer tuples read.
// It assumes that blocks of the outer table are read M pages at a time

const Status QU_Hash_Join(const string & result, 
		     const int projCnt, 
		     const attrInfo projNames[],
		     const attrInfo *attr1, 
		     const Operator op, 
		     const attrInfo *attr2)
{
    Status status;
    int resultTupCnt = 0;
	

    if (attr1->attrType != attr2->attrType ||
        attr1->attrLen != attr2->attrLen)
    {
        return ATTRTYPEMISMATCH;
    }
    
    // go through the projection list and look up each in the 
    // attr cat to get an AttrDesc structure (for offset, length, etc)
    AttrDesc attrDescArray[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        Status status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         attrDescArray[i]);
        if (status != OK)
        {
            return status;
        }
    }
    
    // get AttrDesc structure for the first join attribute
    AttrDesc attrDesc1;
    status = attrCat->getInfo(attr1->relName, attr1->attrName, attrDesc1);
    if (status != OK) return status;

    // get AttrDesc structure for the second join attribute
    AttrDesc attrDesc2;
    status = attrCat->getInfo(attr2->relName, attr2->attrName, attrDesc2);
    if (status != OK) return status;

    // get output record length from attrdesc structures
    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += attrDescArray[i].attrLen;
    }
    
    // open the result table
    InsertFileScan resultRel(result, status);
    if (status != OK) { return status; }

    char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;

    // calculate size of each outer tuple
    AttrDesc *attrs;
    int attrCnt;

    // get attribute data
    if ((status = attrCat->getRelInfo(attr1->relName, attrCnt, attrs)) != OK) return status;

    // compute length of each outer tuple
    int outerTupwidth = 0;
    for (int i = 0; i < attrCnt; i++) 
    {
	outerTupwidth = outerTupwidth + attrs[i].attrLen;
    }
    free(attrs);

    int BLOCKSIZE = 4; // hack to set number of pages in each block of the outer table

    // calculate number of outertuples per page
    int outerTupsPerPage = (PAGESIZE - DPFIXED)/outerTupwidth;
    // finally compute number of tuples in blockSize pages 
    int outerTupsPerBlock = BLOCKSIZE * outerTupsPerPage;

    // open the outer table.  the outer table actually gets opened
    // twice.  Once as a HeapFile and once as a HeapFileScan.
    // The heapfilescan is used to scan the outer table.  The heapfile
    // is used to retrieve tuples that match a given inner tuple

    HeapFile outerTable(string(attrDesc1.relName), status);
    if (status != OK)  return status; 

    // then start scan on outer table
    HeapFileScan outerScan(string(attrDesc1.relName), status);
    if (status != OK)  return status; 
    status = outerScan.startScan(0, 0, STRING, NULL, EQ);
    if (status != OK)  return status; 
    
    // scan outer table
    RID outerRID;
    Record outerRec;
    
    Operator myop;
    switch(op) {
      case EQ:   myop=EQ; break;
      case GT:   myop=LT; break;
      case GTE:  myop=LTE; break;
      case LT:   myop=GT; break;
      case LTE:  myop=GTE; break;
      case NE:   myop=NE; break;
    }

    RID *matchingOuterRids; // actually a variable length array
    int outerRidCnt;

    char* innerJoinAttrPtr;
    joinHashTbl* joinHT;

    bool endOfOuter = false;
    while (!endOfOuter)
    {
   	// allocate and initialize the  hash table
        joinHT = new joinHashTbl ((int) (outerTupsPerBlock * 1.15), attrDesc1);
	int i=0;
	// process the next block of the other table
	while (i < outerTupsPerBlock)
	{
	     // get next outer tuple
	     if (outerScan.scanNext(outerRID) == OK)
	     {
		i++;
		// fetch outer tuple
        	status = outerScan.getRecord(outerRec);
        	ASSERT(status == OK);

		// insert (RID, joinAttrValue) into hash table. The hashtable code 
		// actually does the job of extracting the join attribute value from tuple)
		status = joinHT->insert(outerRID, (char *) outerRec.data);
        	ASSERT(status == OK);
	     }
	     else 
	     {
		endOfOuter = true;
		break; 
	     }
	}
	//printf("processed next block of outer with %d tuples. start scan of inner\n", i);

        // scan inner table
        HeapFileScan innerScan(string(attrDesc2.relName), status);
        if (status != OK)  return status; 

        status = innerScan.startScan(0, 0, STRING, NULL, EQ);
        if (status != OK)  return status; 

        RID innerRID;
        while (innerScan.scanNext(innerRID) == OK)
        {
            Record innerRec;
            status = innerScan.getRecord(innerRec);
            ASSERT(status == OK);

            innerJoinAttrPtr = ((char *)innerRec.data) + attrDesc2.attrOffset,
            // get matching outer rids
	    outerRidCnt = 0;
	    status = joinHT->lookup(innerJoinAttrPtr, outerRidCnt, matchingOuterRids);
            ASSERT(status == OK);
	    if (outerRidCnt > 0)
	    {
	    	// now do the join between the inner tuple and the matching outer tuples (if any)
	    	for (int j=0; j < outerRidCnt; j++)
	    	{
		    // get each next matching outer record
		    // printf("rid of next matching outer tuple is %d.%d\n", 
		    //	matchingOuterRids[j].pageNo, matchingOuterRids[j].slotNo);
		    status = outerTable.getRecord(matchingOuterRids[j], outerRec);
            	    ASSERT(status == OK);
	            //printf("retrieved matching outer tuple from buffer pool\n");

		    // produce an output tuple.   copy data into the output record
		    // from both tuples
                    int outputOffset = 0;
                    for (int k = 0; k < projCnt; k++)
                    {
                    	// copy the data out of the proper input file (inner vs. outer)
                    	if (0 == strcmp(attrDescArray[k].relName, attrDesc1.relName))
                    	{
                           memcpy(outputData + outputOffset,
                           (char *)outerRec.data + attrDescArray[k].attrOffset,
                           attrDescArray[k].attrLen);
                    	}
                    	else // get data from the inner record
                    	{
                           memcpy(outputData + outputOffset,
                           (char *)innerRec.data + attrDescArray[k].attrOffset,
                           attrDescArray[k].attrLen);                    
                    	}
                    	outputOffset += attrDescArray[k].attrLen;
		    }
                    // insert the output tuple into the output relation
                    RID outRID;
                    status = resultRel.insertRecord(outputRec, outRID);
                    ASSERT(status == OK);
		    resultTupCnt++;
                } 
	        free(matchingOuterRids); // release rid vector
	    }
        } // end scan inner

	// all done with current block of the outer table
	innerScan.endScan(); // close the current scan on the inner
	delete joinHT; // delete the join hashtable
    } // end scan outer
    outerScan.endScan();
    printf("blockNL Hash join produced %d result tuples \n", resultTupCnt);
    return OK;
}

const Status QU_Join(const string & result, 
		     const int projCnt, 
		     const attrInfo projNames[],
		     const attrInfo *attr1, 
		     const Operator op, 
		     const attrInfo *attr2)
{

  if ((JoinMethod == NLJoin) || ((JoinMethod == HashJoin) && (op != EQ)))
  {
	return QU_NL_Join (result, projCnt, projNames, attr1, op, attr2);
  }
  else
  if (JoinMethod == SMJoin)
  {
	return QU_SM_Join (result, projCnt, projNames, attr1, op, attr2);
  }
  else return QU_Hash_Join (result, projCnt, projNames, attr1, op, attr2);
}



const int matchRec(const Record & outerRec,
		   const Record & innerRec,
		   const AttrDesc & attrDesc1,
		   const AttrDesc & attrDesc2)
{
  int tmpInt1, tmpInt2;
  float tmpFloat1, tmpFloat2;

  switch(attrDesc1.attrType)
    {
    case INTEGER:
      memcpy(&tmpInt1, (char *)outerRec.data + attrDesc1.attrOffset, sizeof(int));
      memcpy(&tmpInt2, (char *)innerRec.data + attrDesc2.attrOffset, sizeof(int));
      return tmpInt1 - tmpInt2;

    case FLOAT:
      memcpy(&tmpFloat1, (char *)outerRec.data + attrDesc1.attrOffset, sizeof(float));
      memcpy(&tmpFloat2, (char *)innerRec.data + attrDesc2.attrOffset, sizeof(float));
      return int(tmpFloat1 - tmpFloat2);

    case STRING:
      return strcmp((char *)outerRec.data + attrDesc1.attrOffset, 
		    (char *)innerRec.data + attrDesc2.attrOffset);
    }

  return 0;
}
