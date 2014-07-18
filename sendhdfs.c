#include "hdfs.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <libgen.h>
#include <unistd.h>
#include <stdbool.h>
#include <getopt.h>

/*char* loopBufReading( char* rbuf ) 
{
	char* tmpbuf = rbuf;
    int count = 0;
    while(count <= 10) {
        char* tbuf = (char*)malloc(sizeof(char*) * MAXLEN);
        tbuf=fgets(tbuf, READSIZE, stdin);
        if(tbuf!=NULL) {
            strncpy(tmpbuf, tbuf, strlen(tbuf));
            tmpbuf += strlen(tbuf) * sizeof(*tbuf);
            free(tbuf);
        }else{
            printf("No more input!!\n");
            return rbuf;
        }
        count++;
    }
    return rbuf;
}
*/

static int
HDFSFileExists(hdfsFS fs, const char *name) {
	int fileExist;
	fileExist = hdfsExists(fs, name);
	if (fileExist >=  -1) {
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

/*
 *set filename template:%Y-%m-%d-%H-ip.log
 */
char* generatefilename(void) {
    time_t timenow = time(NULL);
    struct tm* p = localtime(&timenow);
    char* buf = (char*)malloc(1024);
    strftime(buf,256,"/rsyslogtest/%Y-%m-%d-%H-172.18.218.19.log",p);
    return buf;
    free(buf);
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
        ,cmd);
    exit(1);
}

int main(int argc, char *argv[]) {
	
    char* filename = generatefilename();
    char* hostname;
    int portnum;
    char* username;
    int opt;

    while((opt = getopt(argc, argv, "hd:p:u:")) !=-1) {
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
	re = HDFSFileExists(fs, filename);
    if(re == 1) {
        printf("file path exits!!\n");
    }
	free(file_t);
    
    hdfsFile fh = HDFSopenfile(fs, filename, re);
	if (fh == NULL) {
		printf("error open return\n");
		exit(-1);
	}
    
    char* rbuf=(char *)malloc(sizeof(char*) * 1024);
    if (rbuf == NULL) {
        return -2;
    }
    char* mbuf=rbuf;
    while((fgets(mbuf, 1024, stdin)) != NULL ) {
	    hdfsWrite(fs, fh, (void*)rbuf, strlen(rbuf));
        int le;
        le = hdfsFlush(fs, fh);
        if (le != 0) {
            fprintf(stderr, "Faliede to flush \n");
        }   
    }
    free(rbuf);

    hdfsCloseFile(fs, fh);
    hdfsDisconnect(fs);
    return 0;
}
