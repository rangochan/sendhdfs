#include "hdfs.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <libgen.h>
#include <unistd.h>
#include <stdbool.h>
#include <getopt.h>
#define READSIZE 30 
#define MAXLEN 10

char* loopBufReading( char* rbuf ) 
{
	char* tmpbuf = rbuf;
    char* tbuf = (char*)malloc(sizeof(char*) * MAXLEN);

    int be_return = 0;
    while(1) {
        tbuf=fgets(tbuf, READSIZE, stdin);
        if(tbuf!=NULL) {
            if(READSIZE < strlen(rbuf) + strlen(tbuf)) {
                be_return ++;
            }
            strncpy(tmpbuf, tbuf, strlen(tbuf));
            tmpbuf += strlen(tbuf) * sizeof(*tbuf);
            if(be_return > 0) {
                return rbuf;
            }
        }else{
            printf("No more input!!\n");
            return rbuf;
        }
    }
}

static int
HDFSFileExists(hdfsFS fs, const char *name) {
	int fileExist;
	fileExist = hdfsExists(fs, name);
	if (fileExist >= -1) {
		return ++fileExist;
	} else {
		printf("Please specify a correct hdfs file path");
		return -2;
		}
}

hdfsFile HDFSopenfile(hdfsFS fs, const char *filename, int re) {
	hdfsFile writeFile;
	if (re == 1) {
		printf(" file existing ,and append data to it \n");
		writeFile = hdfsOpenFile(fs, filename, O_WRONLY | O_APPEND, 3, 3, 1024);
	} else {
		printf(" file is not existing, and create a new file \n");
		writeFile = hdfsOpenFile(fs, filename, O_WRONLY, 3, 3, 1024);
	}

	if (writeFile == NULL) {
		fprintf(stderr, "Failed to open %s for writing!\n", filename);
		exit(-1);
	}
	return writeFile;
}

void usage(const char * cmd)
{
    fprintf(stderr, "Usage: %s [-h] | [-d <hostname>] \n"
            "\n"
            " Options:\n"
            "  -h                print this help message\n"
            "  -d <hostname>     hdfs server hostname or ip\n"
            "  -p <portnum>       connetion port number\n"
            "  -u <username>     username for connnection\n"
            "  -f <filename>     name of file written\n"
            "  -s <fileMaxSize>  max written data bytes\n"
        ,cmd);
    exit(1);
}

int main(int argc, char *argv[]) {
	
    char* filename;
    tSize fileMaxSize;
    char* hostname;
    int portnum;
    char* username;
    int opt;

    while((opt = getopt(argc, argv, "hd:p:u:f:s:")) !=-1) {
        switch(opt) {
            case 'd':
                hostname = optarg;
                break;
            case 'p':
                portnum = atoi(optarg);
                break;
            case 'u':
                username = optarg;
                break;
            case 'f':
                filename = optarg;
                break;
            case 's':
                fileMaxSize = atoi(optarg);
                break;
            case 'h':
            default:
                usage(argv[0]);
                exit(-1);
                break;
        }
    }

    char *file_t = strdup(filename);
	char *path = dirname(file_t);
	printf("dirname %s \nfilename %s\n", path, filename);

    hdfsFS fs = hdfsConnectAsUser(hostname, portnum, username);
	if(fs == NULL){
		printf("please specify a correct hadoop connection\n");
	}
	
    int re;
	re = HDFSFileExists(fs, path);
    if(re = 1) {
        printf("file path exits!!\n");
    }
	free(file_t);
	
    hdfsFile fh = HDFSopenfile(fs, filename, re);
	if (fh == NULL) {
		printf("error open return\n");
		exit(-1);
	}
    
    tSize writetotalsize=0,writesize=0;
	while(writetotalsize < fileMaxSize) {
        char* rbuf=(char *)malloc(sizeof(char*) * READSIZE);
        if (rbuf == NULL) {
            return -2;
        }

        char* mbuf;
        mbuf = loopBufReading(rbuf);
		writesize = hdfsWrite(fs, fh, (void*)rbuf, strlen(rbuf));
		writetotalsize += writesize;
        
        int le;
        le = hdfsFlush(fs, fh);
        if (le != 0) {
            fprintf(stderr, "Failed to flush \n");
        }
        free(rbuf);
    }
    hdfsCloseFile(fs, fh);
    hdfsDisconnect(fs);
    return 0;
}
