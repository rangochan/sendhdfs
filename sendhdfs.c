#include "hdfs.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <libgen.h>
#include <unistd.h>
#include <stdbool.h>
#include <getopt.h>

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
 *set filename template:ip.%H.log
 */
char* generatefilename(time_t tnow) {
    struct tm* p = localtime(&tnow);
    char* buf = (char*)malloc(100);
    strftime(buf,100,"/172.18.218.19.%H.log",p);
    return buf;
    free(buf);
}

/*
 *set file parent path template:/%Y-%m-%d/
 */
char* generatepath(time_t tnow) {
    struct tm* p = localtime(&tnow);
    char* buf = (char*)malloc(100);
    strftime(buf,100,"/rsyslogtest/%Y-%m-%d",p);
    return buf;
    free(buf);
}

void usage(const char * cmd)
{
    fprintf(stderr, "Usage: %s [-h] | [-d <hostname>] | [-p <portnum>] | [-u hadoop]\n"
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
	time_t init_time = time(NULL);
    char* filename = generatefilename(init_time);
    char* logpath = generatepath(init_time);
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
                if ( atoi(optarg) < 1024 || atoi(optarg) > 65535 ) {
                    portnum = 8020;
                } else {
                    portnum = atoi(optarg);
                }
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

    /* produce a hdfs connection !!*/
    hdfsFS fs = hdfsConnectAsUser(hostname, portnum, username);
	if(fs == NULL){
		printf("please specify a correct hadoop connection\n");
	}
	
    /* make the log directory depending on current date */
    int mk = hdfsCreateDirectory( fs, logpath);
    printf("%d,%d\n",__LINE__,mk);
    if (mk != 0) {
        fprintf(stderr,"can't make the log directory!!");
    } else {
        printf("current day's log directory is %s\n",logpath);
        
    }

    char* filepath=strcat(logpath,filename);
    /* hdfs file exists? */
    int re;
	re = HDFSFileExists(fs, filepath);
    if(re == 1) {
        printf("file path exits!!\n");
    }
    
    /* open a hdfs file depending on the value of re */
    hdfsFile fh = HDFSopenfile(fs, filepath, re);
	if (fh == NULL) {
		printf("error open return\n");
		exit(-1);
	}
    
    char* rbuf=(char *)malloc(sizeof(char*) * 1024);
    if (rbuf == NULL) {
        return -2;
    }
    char* mbuf=rbuf;

    /* loop read stdin and write into hdfs file until fgets encounts NULL ! */
    while((fgets(mbuf, 1024, stdin)) != NULL ) {
	    time_t next_time = time(NULL);

        /* is already the next day ?*/
        if ((init_time /86400) == (next_time/86400)) {
            /* is already the next hour ? */
            if ((init_time/3600) == (next_time)/3600) {
                hdfsWrite(fs, fh, (void*)rbuf, strlen(rbuf));
                hdfsFlush(fs,fh);
                int le;
                if(le != 0) {
                    fprintf(stderr, "Failed to flush \n");
                }
            }else {
                hdfsCloseFile(fs,fh);
                char* curfilepath = generatepath(next_time);
                char* nextfile = generatefilename(next_time);
                char* nextfilepath=strcat(curfilepath,nextfile);
                hdfsFile fh = HDFSopenfile(fs, nextfilepath, 2);
                hdfsWrite(fs,fh,(void*)rbuf,strlen(rbuf));
                hdfsFlush(fs,fh);
            }
        }else {
                hdfsCloseFile(fs, fh); 
                char* curfilepath = generatepath(next_time);
                char* nextfile = generatefilename(next_time);
                char* nextfilepath=strcat(curfilepath,nextfile);
                hdfsCreateDirectory(fs,curfilepath);
                hdfsFile fh = HDFSopenfile(fs,nextfilepath, 2);
                hdfsWrite(fs,fh,(void*)rbuf,strlen(rbuf));
                hdfsFlush(fs,fh);
        }
        init_time = next_time;
    }
    free(rbuf);

    /* close hdfs file and diconnect! */
    hdfsCloseFile(fs, fh);
    hdfsDisconnect(fs);    
    return 0;
}
