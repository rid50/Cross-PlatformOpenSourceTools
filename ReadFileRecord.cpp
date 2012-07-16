#include <fstream>
#include <iostream>    // cin, cout objects & methods
#include <vector>
#include <sstream>

#include <stdio.h>     // FILE structure
#include <dirent.h>

#include <wx/chartype.h>
#include <wx/textctrl.h>
#include <wx/log.h>
#include <wx/stopwatch.h>
#include "wx/thread.h"

#include <boost/regex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>
#include <boost/format.hpp>

#include "BoostSerialization.h"
#include "MyThread.h"

using std::cout;
using std::endl;
using std::string;
using std::vector;

#if !wxUSE_THREADS
    #error "This sample requires thread support!"
#endif // wxUSE_THREADS

//extern wxTextCtrl *logWin;
extern wxThreadEvent myEvent;

boost::mutex mutex;

void compareFiles(wxFrame* frame, string fileName, string fileName2, record_field_list<boost::shared_ptr<field_name_length> > *r_schema)
{
    //record record;
    FILE *pFile;
    FILE *pFile2;
    short recordsRead;
    short recordsRead2;
    short NUM_RECORDS = 1000;
    wxString log = "";
    char *buffer;
    char *buffer2;
    char *pRecord;
    char *pRecord2;

    std::stringstream sstm;

    wxStopWatch sw;

    std::vector<boost::shared_ptr<field_name_length> > schema = r_schema->getList();

    int record_size = 0, key_size = 0, value_size = 0;

    for (unsigned int i = 0; i < schema.size(); i++) {
        if (schema[i]->key) {
            key_size += schema[i]->len;
        } else if (!schema[i]->black) {
            value_size += schema[i]->len;
        }
        record_size += schema[i]->len;
    }

    record_size++;  // lf 0x0A

    typedef boost::unordered_map<std::string, std::string> unordered_map;
    //typedef std::map<std::string, std::string> unordered_map;
    unordered_map map;
    unordered_map map2;

    pFile = fopen((char *)fileName.c_str(), "rb");
    pFile2 = fopen((char *)fileName2.c_str(), "rb");
    if (pFile && pFile2)
    {
        log += wxString::Format("Process file: %s", fileName) + _T("\n");

        // obtain file size:
        fseek (pFile, 0, SEEK_END);
        long fileSize = ftell(pFile);
        rewind(pFile);

        buffer = (char*)malloc(record_size * NUM_RECORDS);
        buffer2 = (char*)malloc(record_size * NUM_RECORDS);
        if (buffer == NULL || buffer2 == NULL) {
            fclose(pFile);
            fclose(pFile2);

            log += wxString("Cannot allocate buffer") + _T("\n");
            exit (1);
        }

        recordsRead = fread(buffer, record_size, NUM_RECORDS, pFile);
        recordsRead2 = fread(buffer2, record_size, NUM_RECORDS, pFile2);

        char *key = (char*)malloc(key_size);
        char *value = (char*)malloc(value_size);
        char *key2 = (char*)malloc(key_size);
        char *value2 = (char*)malloc(value_size);

        if (key == NULL || key2 == NULL || value == NULL || value2 == NULL) {
            fclose(pFile);
            fclose(pFile2);

            log += wxString("Cannot allocate key/value pairs") + _T("\n");
            exit (1);
        }

        int n = 0; int dupKeys = 0;
        string strKey;
        string strValue;
        string strKey2;
        string strValue2;
        std::pair<unordered_map::iterator, bool> mapInsertResult;
        while ( recordsRead != 0 && recordsRead2 != 0 )
        {

            for (short i = 0; i < recordsRead; i++, n++) {
                pRecord = buffer + i * record_size;
                pRecord2 = buffer2 + i * record_size;

                char *pKey = key;
                char *pValue = value;
                char *pKey2 = key2;
                char *pValue2 = value2;
                //char *pBuffer = buffer;
                for (unsigned int k = 0; k < schema.size(); k++) {
                    if (schema[k]->key) {
                        memcpy (pKey, pRecord, schema[k]->len);
                        pKey += schema[k]->len;
                        memcpy (pKey2, pRecord2, schema[k]->len);
                        pKey2 += schema[k]->len;
                        //keys[j++] = buffer[schema[i]->len];
                    } else if (!schema[k]->black) {
                        memcpy (pValue, pRecord, schema[k]->len);
                        pValue += schema[k]->len;
                        memcpy (pValue2, pRecord2, schema[k]->len);
                        pValue2 += schema[k]->len;
                    }

                    pRecord += schema[k]->len;
                    pRecord2 += schema[k]->len;
                }

                strKey = string(key, key_size);
                strValue = string(value, value_size);
                mapInsertResult = map.insert(unordered_map::value_type(strKey, strValue));
                if (!mapInsertResult.second) {
                    log += wxString::Format("Duplicate key in file: %s", wxString(fileName.c_str())) + _T("\n");
                    log += wxString::Format("Key field: %s", wxString(strKey)) + _T("\n");
                    dupKeys++;
                    continue;
                }
                // else {
                //    strValue = string(value, value_size);
                //}

                strKey2 = string(key2, key_size);
                strValue2 = string(value2, value_size);
                mapInsertResult = map2.insert(unordered_map::value_type(strKey2, strValue2));
                if (!mapInsertResult.second) {
                    log += wxString::Format("Duplicate key in file: %s", wxString(fileName2.c_str())) + _T("\n");
                    log += wxString::Format("Key field: %s", wxString(strKey2)) + _T("\n");
                }
                // else {
                //    strValue2 = string(value2, value_size);
                //}
            }
            recordsRead = fread(buffer, record_size, NUM_RECORDS, pFile);
            recordsRead2 = fread(buffer2, record_size, NUM_RECORDS, pFile2);
        }  // end of loop

        free(key);
        free(value);
        free(key2);
        free(value2);
        free(buffer);
        free(buffer2);

         // All of the file data has now been processed
         // We can close the file
        fclose(pFile);
        fclose(pFile2);

        for (unordered_map::iterator it = map.begin(); it != map.end(); ++it) {
            //char * lo;
            //sprintf(lo, "%s", it->first.c_str());
            //log += wxString(string(lo)) + _T("\n");
            //std::string s = str(boost::format("%s") % string(it->first.c_str(), 6));
            //log += wxString(s) + _T("\n");
            //log += wxString::Format("%s", wxString(it->first, 6)) + _T("\n");
            unordered_map::iterator it2 = map2.find(it->first);
            if (it2 != map2.end()) {
                if (it->second != it2->second) {
                    short pos = 0;
                    for (unsigned int k = 0; k < schema.size(); k++) {
                        if (!(schema[k]->key || schema[k]->black)) {
                            for (unsigned int m = 0; m < schema[k]->len; m++) {
                                if ((it->second.c_str()[pos + m] ^ it2->second.c_str()[pos + m]) != 0) {
                                    string key;
                                    boost::regex expr("[\\W|\\D|\\S]"); //"[^\x20-\x7E]"
                                    key = boost::regex_replace(it->first, expr, "?");
                                    log += wxString::Format("Fields %s do not match, key field: %s, old value: %s, new value: %s",
                                                            schema[k]->name,
                                                            wxString(key),
                                                            wxString(it->second.c_str() + pos, schema[k]->len),
                                                            wxString(it2->second.c_str() + pos, schema[k]->len)) + _T("\n");
                                }
                            }

                            pos += schema[k]->len;
                        }
                    }
                }
            } else {
                string key;
                boost::regex expr("[\\W|\\D|\\S]"); //"[^\x20-\x7E]"
                key = boost::regex_replace(it->first, expr, "?");
                log += wxString::Format("No record found: key field: %s", wxString(key)) + _T("\n");

//                //sstm << str(boost::format("No record found: key field: %s\n") % it->first);
//                char keyString[it->first.length() * 2];
//                //char keyString2[] = "abcd";
//                //for (int m = 0, k = 0; keyString2[m] != '\0'; m++, k = k + 2) {
//                //    sprintf(keyString + k, "%x", keyString2[m]);
//                //}
//                for (int m = 0, k = 0; it->first.c_str()[m] != '\0'; m++, k = k + 2) {
//                    sprintf(keyString + k, "%x", it->first.c_str()[m]);
//                }
//                //strcpy (cstr, str.c_str());
//
//                log += wxString::Format("No record found: key field: %s", wxString(keyString, it->first.length() * 2)) + _T("\n");
            }
        }

        map.clear();
        map2.clear();

        log += wxString::Format("Time to process the file: %ldms", sw.Time()) + _T("\n");
        log += wxString::Format("Number of duplicate keys: %d", dupKeys) + _T("\n");
        log += wxString::Format("Number of records: %d", n) + _T("\n");
        log += wxString::Format("File size reported by a file system: %d", fileSize) + _T("\n");
        log += wxString::Format("File size calculated (641 x %d): %d", n, record_size * n) + _T("\n");
    }     // end of normal program
    else  // File cannot be opened !
    {
        if (pFile == 0)
            log += wxString::Format("Unable to open file: %s", fileName) + _T("\n");
        else
            fclose(pFile);

        if (pFile2 == 0)
            log += wxString::Format("Unable to open file: %s", fileName2) + _T("\n");
        else
            fclose(pFile2);
    }

    //string ss = sstm.str();
    mutex.lock();
    myEvent.SetString(log);  // pass some data along the event
    wxQueueEvent(frame, myEvent.Clone());
    //myEvent.SetString(wxString(sstm.str()));  // pass some data along the event
    //wxQueueEvent(frame, myEvent.Clone());
    mutex.unlock();
}

int listDirectory(char *dir, vector<string> &file_list, string fileNamePattern)
{
    DIR *dp;
    struct dirent *ep;

    boost::regex expr(fileNamePattern);

    dp = opendir(dir);
    if (dp != NULL)
    {
        while((ep = readdir(dp))) {
            if (boost::regex_match(string(ep->d_name), expr)) {
                file_list.push_back(string(ep->d_name));
            }
        }

        (void)closedir(dp);

        //vector<string>::iterator it;

        //sort (file_list.begin(), file_list.end());
    }
    else
        return 1;

    return 0;
}

int compare(wxFrame* frame, string fileSchema)
{
    wxString log = "";
    record_field_list<boost::shared_ptr<field_name_length> > nsn_r4;

    // restore data from archive
    {
        std::ifstream ifs(fileSchema.c_str());
        boost::archive::xml_iarchive ia(ifs);
        // write class instance to archive
        ia >> BOOST_SERIALIZATION_NVP(nsn_r4);
    	// archive and stream closed when destructors are called
    }

    wxStopWatch sw;

    string dirName = "c:/mediation_streams/old_system/Output" + nsn_r4.getOutputDir();
    string dirName2 = "c:/mediation_streams/new_system/Output" + nsn_r4.getOutputDir();

    vector<string> file_list = vector<string>();
    if (listDirectory((char *)dirName.c_str(), file_list, nsn_r4.getFileNamePattern())) {
        mutex.lock();
        myEvent.SetString(wxString::Format("Cannot open directory: %s", dirName) + _T("\n"));  // pass some data along the event
        wxQueueEvent(frame, myEvent.Clone());
        mutex.unlock();
        return 1;
    }

    std::sort(file_list.begin(), file_list.end());

    std::stringstream sstm;

    string fileName, fileName2;
    for (unsigned int i = 0; i < file_list.size(); i = i + 3) {
        sstm.str("");
        sstm << dirName << file_list[i];
        fileName = sstm.str();

        sstm.str("");
        sstm << dirName2 << file_list[i];
        fileName2 = sstm.str();

        boost::thread t1(compareFiles, frame, fileName, fileName2, &nsn_r4);

        if (i + 1 < file_list.size()) {
            sstm.str("");
            sstm << dirName << file_list[i + 1];
            fileName = sstm.str();

            sstm.str("");
            sstm << dirName2 << file_list[i + 1];
            fileName2 = sstm.str();

            boost::thread t2(compareFiles, frame, fileName, fileName2, &nsn_r4);

            if (i + 2 < file_list.size()) {
                sstm.str("");
                sstm << dirName << file_list[i + 2];
                fileName = sstm.str();

                sstm.str("");
                sstm << dirName2 << file_list[i + 2];
                fileName2 = sstm.str();

                boost::thread t3(compareFiles, frame, fileName, fileName2, &nsn_r4);
                t3.join();
            }
            t2.join();
        }
        t1.join();
    }

    //mutex.lock();
    //myEvent.SetString(wxString::Format("Time to process all files: %ldms", sw.Time()) + _T("\n"));  // pass some data along the event
    //wxQueueEvent(frame, myEvent.Clone());
    //mutex.unlock();

    return 0;
}
