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
#include <assert.h>

static char* listlogfile = "/var/log/listlog.log";
/* read configuration file */
int read_config(const char * key, char * value, int size, const char * file)
{
	char  buf[1024] = { 0 };
	char * start = NULL;
	char * end = NULL;
	int  found = 0;
	FILE * fp = NULL;
	int keylen = strlen(key);

	// check parameters
	assert(key  !=  NULL  &&  strlen(key)); 
	assert(value  !=  NULL);
	assert(size  >   0 );
	assert(file  != NULL  && strlen(file));


	if (NULL != (fp = fopen(file, "r"))) {
		while (fgets(buf, sizeof(buf), fp)) {
			start = buf;
			while (*start == ' ' || *start == '\t') start++;
			if (*start == '#') continue;
			if (strlen(start) <= keylen) continue;
			if (strncmp(start, key, keylen)) continue;
			if (start[keylen] != ' ' && start[keylen] != '\t' && start[keylen] != '=')
				continue;
			start += keylen;
			while (*start == '=' || *start == ' ' || *start == '\t') start++;
			end = start;
			while (*end && *end != '#' && *end != '\r' && *end != '\n') end++;
			*end = '\0';
			strncpy(value, start, size);  
			value[size-1] = '\0';
			found = 1;
			break;
		}
		fclose(fp);
		fp = NULL;
	}

	if (found) {
		return strlen(value);
	} else {
		return 0;
	}
}

/* define a linked list used for storing data read from stdin */
int listsize = 0;
typedef struct Node {
	char* value;
	struct Node *next;
}listNode;

/* init list */
void initList(listNode** listHead) {
	*listHead = (listNode*)malloc(sizeof(listNode));
	if(listHead == NULL) {
		printf("failed to malloc!\n");
	}
	(*listHead)->next=NULL;
	(*listHead)->value=NULL;
}

/* insert data into list behind head node*/
void listInsert(listNode *head,char*s) {
   if(NULL==head ||NULL==s)
   {
		return;
   }
   
   int len=strlen(s);
   listsize += (len+1);
   
   listNode *p;
   p = (listNode*)malloc(sizeof(listNode));
   if(NULL==p)
   {    
        printf("failed to malloc!\n");    
   }

   p->value=(char*)malloc(len+2);
   memset(p->value,'\0',len+1);
   if(NULL==p->value)
   {
		printf("failed to malloc!\n");
   }
   memcpy(p->value,s,len);
   
   p->next=head->next;
   head->next=p;  
}

/* clear all of list node besides head node */
void clearList(listNode* pHead) {
	listNode* pNext;

	if(pHead == NULL) {
		printf("list is empty,can't clear!\n");
		return;
	}
	listNode* p=pHead->next;
	
	while(p!= NULL) {
		pNext=p;
		p=p->next;
		free(pNext->value);
		p->value=NULL;
		free(p);
		p=NULL;
		
	}
    pHead = NULL;
    listsize = 0;
	printf("succed to clear list node besides head node!\n ");
}

/* delete the first node of list besides head node*/
void delfirstNode(listNode* pHead)
{
     if(NULL!=pHead) {
	 	return;
	 }
	 
     listNode* p=pHead->next;
     if(NULL!=p)
     {
         pHead->next=p->next;
     }
     listsize-=(strlen(p->value)+1);
     free(p);
     p=NULL;
}


/* obtain the data of the first node */
char *getnodeValue(listNode*pHead)
{
   	if(NULL==pHead  || NULL==pHead->next)
   	{
   		return NULL;
   	}
   	listNode* pNext=pHead->next;
   	return pNext->value;
}

/* free a certain node */
void freeNode(listNode *p )
{
     if(NULL == p) 
	 	return;
     free(p);
     p=NULL;
}

/* delete all of list node include head node */
void delList(listNode* pHead) {
	if(NULL==pHead)
		return;
	clearList(pHead);
	free(pHead);
	pHead=NULL;
}

/* write list remain data into a local file when main exit */
void writelistData(listNode* pHead) {
	int fd=open(listlogfile, O_WRONLY|O_APPEND|O_CREAT, 0666);
	if(fd == !-1)
		return;
	char* buffer=NULL;
	listNode* p=pHead->next;
	int len = 0;
	while(NULL != (buffer = getnodeValue(p))) {
		len=strlen(buffer);
		buffer[len]='\n';
		buffer[len+1]='\0';
		write(fd, buffer, strlen(buffer));
		p=p->next;
		buffer=NULL;
	}
	close(fd);
}

/* read file and write file's data into list */
void writefiletoList(char* pathname, listNode* pHead)
{
     int fd=open(pathname,O_RDONLY|O_CREAT,0666);
     assert(-1 != fd);
     char buf[1024]={0};
     while(read(fd,buf,1023) > 0)
     {
        listInsert(pHead, buf);
        memset(buf,'\0',1024);
     }      
     close(fd);
}


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
        char* p = "%";
        char* str_buf = strdup(str);
        char* token;
        
        for( token = strsep(&str_buf, p); token != NULL; token = strsep(&str_buf, p)) {
            proper[i] = strdup(token);

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
                                        char flag_date[] = "%F";
                                        char* date = timestr(flag_date);
                                        proper[i] = date;
                                    } else {
                                        if ( (strcmp(proper[i], "HOUR")) == 0 ) {
                                            char flag_h[] = "%H";
                                            char* hour = timestr(flag_h);
                                            proper[i] = hour;
                                        }
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
        memset(tmp,'\0',1024);
        for( j; j< i; j++ ) {
            strcat(tmp, proper[j]);
            free(proper[j]);
        }
        free(str_buf);

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
            "  -c <configfile>	 sendhdfs's configuration file\n"
            "  -d <hostname>     hdfs server hostname or ip\n"
            "  -p <portnum>       connetion port number\n"
            "  -u <username>     username for connnection\n"
            "  -t <filetemplatename> file template name\n"
            "  -l <listlogfile>  file written by list remain data\n"
            "  -m <listmaxsize>  list data max size\n"
        ,cmd);
    exit(1);
}

int main(int argc, char *argv[]) {
    
    char* servername = NULL;
    int portnum = 8020;
    char* username="hadoop";
	char* template = NULL; 
    char* filename;
    char* newfilename; 
	int opt;
	listNode* head;
	initList(&head);
    char value[1024] = { 0 };
	int listmaxsize = 1024*1024;

	printf("test \n");	
	if (read_config("template", value, sizeof(value), "/etc/sendhdfs.conf") > 0) {
		template = strdup(value);
        memset(value,'\0',1024);
	}
	if (read_config("servername", value, sizeof(value), "/etc/sendhdfs.conf") > 0) {
		servername = strdup(value);
         memset(value,'\0',1024);
	}
	if (read_config("portnum", value, sizeof(value), "/etc/sendhdfs.conf") > 0) {
		portnum = atoi(value);
		if (portnum < 1024 || portnum > 65535) {
			portnum = 8020;
		}
	}
  
    if (read_config("username", value, sizeof(value), "/etc/sendhdfs.conf") > 0) {
		username = strdup(value);
		memset(value,'\0',1024);
	}

	if (read_config("listmaxsize", value, sizeof(value), "/etc/sendhdfs.conf") > 0) {
		listmaxsize = atoi(value);
	}
	if (read_config("listlogfile", value, sizeof(value), "/etc/sendhdfs.conf") > 0) {
		listlogfile = strdup(value);
		memset(value,'\0',1024);
	}
	printf("test1 \n");	
    while((opt = getopt(argc, argv, "hd:p:u:t:c:m:l:")) !=-1) {
        switch(opt) {

			case 'c':
				if (read_config("template", value, sizeof(value), optarg) > 0) {
					template = strdup(value);
        			memset(value,'\0',1024);
				}
				if (read_config("servername", value, sizeof(value), optarg) > 0) {
					servername = strdup(value);
                	memset(value,'\0',1024);
				}
				if (read_config("portnum", value, sizeof(value), optarg) > 0) {
					portnum = atoi(value);
					if (portnum < 1024 || portnum > 65535) {
					portnum = 8020;
					}
				}
    			if (read_config("username", value, sizeof(value), optarg) > 0) {
					username = strdup(value);
					memset(value,'\0',1024);
				}
				if (read_config("listmaxsize", value, sizeof(value), optarg) > 0) {
					listmaxsize = atoi(value);
				}
				if (read_config("listlogfile", value, sizeof(value), optarg) > 0) {
					listlogfile = strdup(value);
					memset(value,'\0',1024);
				}		
				break;

			case 'd':
                if( optarg != NULL)
					servername = optarg;
                break;
            
            case 'p':
				if( optarg != NULL) {
					if ( atoi(optarg) < 1024 || atoi(optarg) > 65535 ) {
                    	portnum = 8020;
                	} else {
                    	portnum = atoi(optarg);
                	}
				}
                break;

            case 'u':
				if( optarg != NULL)
					username = optarg;
                break;

			case 't':
                if( optarg != NULL)
					template = optarg;
				break;

			case 'l':
                if( optarg != NULL)
					strcpy(listlogfile, optarg);
				break;

			case 'm':
                if( optarg != NULL)
					listmaxsize = atoi(optarg);
				break;
            
            case 'h':
            default:
                usage(argv[0]);
                break;
        }
    }
	
	printf("test2 \n");	
    /* convert filetemplatename into filename */ 
    filename = strdup(tpltostr(template)); 
    printf("%s\n", filename);
    
    /* produce a hdfs connection !! */
    hdfsFS fs = hdfsConnectAsUser(servername, portnum, username);
	if(fs == NULL){
		printf("please specify a correct hadoop connection\n");
	}

    free(servername);
    free(username);

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

    /* loop read stdin and write into hdfs file until fgets encounts NULL ! */
    while((fgets(mbuf, 4096, stdin)) != NULL ) {
		if(EINTR==errno) {
			continue;
		}

		if( listsize >= listmaxsize)
			sleep(2);

		listInsert(head,rbuf);
		char* tmpbuf=NULL;
		int len=0;

		if(NULL != (tmpbuf=getnodeValue(head))) {
			len=strlen(tmpbuf);
		}

		newfilename = tpltostr(template);

		/* whether encount a newline? */
        //if ((strchr(mbuf, '\n')) != NULL) {

			/* produced a new filename?  */
		if( (strcmp(newfilename, filename)) == 0 ) {
            hdfsWrite(fs, fh, (void*)tmpbuf, len);
			hdfsHFlush(fs, fh);
		} else {
			hdfsCloseFile(fs, fh);
			fh = HDFSopenfile(fs, newfilename, 2);
			hdfsWrite(fs, fh, (void*)tmpbuf, len);
			hdfsHFlush(fs, fh);
		}
		strcpy(filename, newfilename);
    }
    free(rbuf);

	
    /* close hdfs file and disconnect! */
    hdfsCloseFile(fs, fh);
    hdfsDisconnect(fs);
	
	writelistData(head);
	delList(head);
	free(template);

    return 0;
}
