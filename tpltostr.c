/*************************************************************************
	> File Name: tpltostr.c
	> Author: Rangochan
	> Mail: rangochan1989@gmail.com 
	> Created Time: Fri 25 Jul 2014 03:49:16 PM CST
 ************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
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
    for( j; j< i; j++ ) {
        strcat(tmp, proper[j]);
    }

    free(buf);
    return tmp;
}

int main() {
    char* str="rsyslog.$HOSTNAME$.$PID$.$YEAR$.$MONTH$.$DAY$.$MINUTE$.$DATE$.log";
    char* string = tpltostr(str);
    printf("%s\n", string);
    free(string);
    return 0;
}
