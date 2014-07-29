#include "hdfs.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <libgen.h>
#include <unistd.h>
#include <stdbool.h>
#include <getopt.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>

char* timestr(char* flag) {    
	time_t timenow = time(NULL);    
	struct tm* p = localtime(&timenow);    
	char tmpbuf[56];    
	strftime(tmpbuf, 28,flag,p);    
	char* buf = strdup(tmpbuf);    
	return buf;
}

/* convert template into string */
char* tpltostr(char* str) {        
	char* proper[100];    
	int i = 0;    
	char* p = "$";    
	char* buf = strdup(str);    
	char* token;    
	for( token = strsep(&buf, p); token != NULL; token = strsep(&buf, p)) {              
		proper[i] = token;                
		/* convert property into value */        
		if( (strcmp(proper[i], "HOSTNAME")) == 0 ) {            
			char hostname[128];            
			gethostname(hostname,sizeof(hostname));            
			proper[i] = strdup(hostname);        
			}else {            
				if( (strcmp(proper[i], "PID")) == 0) {                
					int pid = getpid();                
					char pidc[25];                
					sprintf(pidc, "%d", pid);                
					proper[i] = strdup(pidc);            
				} else {                
					if( (strcmp(proper[i], "YEAR")) == 0 ) {                    
						char flag_y[] = "%Y";                    
						char* year = timestr(flag_y);                    
						proper[i] = year;                
					} else {                    
						if( (strcmp(proper[i], "MONTH")) == 0 ) {                        
							char flag_m[] = "%m";                        
							char* month = timestr(flag_m);                        
							proper[i] = month;                    
						} else {                        
							if( (strcmp(proper[i], "DAY")) == 0 ) {                            
								char flag_d[] = "%d";                            
								char* day = timestr(flag_d);                            
								proper[i] = day;                        
							} else {                            
								if( (strcmp(proper[i], "MINUTE")) == 0 ) {                                
									char flag_mi[] = "%M";                                
									char* min = timestr(flag_mi);                                
									proper[i] = min;                            
								} else {                                
									if( (strcmp(proper[i], "DATE")) == 0 ) {                                    
										char flag_da[] = "%D";                                    
										char* date = timestr(flag_da);                                    
										proper[i] = date;                                
									}                            
								}                       
							}                    
						}                
					}            
				}           
			}        
		i++;    
	}

	int j = 0;    
	char* tmp =(char*)malloc(1024);    
	for( j; j< i; j++ )        
		strcat(tmp, proper[j]);    

	free(buf);    
	return tmp;
}


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
		printf(" file exists ,and append data to it \n");
		writeFile = hdfsOpenFile(fs, filename, O_WRONLY | O_APPEND, 3, 3, 1024);
	} else {
		printf(" file not exists, and create a new file \n");
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
    fprintf(stderr, "Usage: %s [-h] | [-d <hostname>] | [-p <portnum>] | [-u hadoop]\n"
            "\n"
            " Options:\n"
            "  -h                print this help message\n"
            "  -d <hostname>     hdfs server hostname or ip\n"
            "  -p <portnum>       connetion port number\n"
            "  -u <username>     username for connnection\n"
            "  -t <filetemplatename> file template name\n"
        ,cmd);
    exit(1);
}

int main(int argc, char *argv[]) {
    
    char* hostname;
    int portnum;
    char* username="hadoop";
    int opt;
	char* filetpl;

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

			case 't':
				filetpl = optarg;
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

	/* convert filetemplatename into filename */
	char* filename = tpltostr(filetpl);

    /* hdfs file exists? */
    int re;
	re = HDFSFileExists(fs, filename);
    if(re == 1) {
        printf("file path exits!!\n");
    }
    
    /* open a hdfs file depending on the value of re */
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

//	char* newfile;
    /* loop read stdin and write into hdfs file until fgets encounts NULL ! */
    while((fgets(mbuf, 4096, stdin)) != NULL ) {
	 
        /* whether encount a newline? */        
        if ((strchr(mbuf, '\n')) != NULL) {
            hdfsWrite(fs, fh, (void*)rbuf, strlen(rbuf));
			hdfsHFlush(fs, fh);
    }
    free(rbuf);

    /* close hdfs file and disconnect! */
    hdfsCloseFile(fs, fh);
    hdfsDisconnect(fs);    
    return 0;
}
