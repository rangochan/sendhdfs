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

/* string replace */
char* strreplace(char* src, char* sub, char* dst) {
    char *p, *p1;
    int len;
                
    if ((src == NULL) || (sub == NULL) || (dst == NULL)) 
        return NULL;

    while( (strstr(src, sub)) != NULL) {
        p = strstr(src, sub);
        len = strlen(src) + strlen(dst) - strlen(sub);
        p1 = malloc(len);
        bzero(p1, len);
        strncpy(p1, src, p - src);
        strcat(p1, dst);
        p += strlen(sub);
        strcat(p1, p);
        src = p1;
    }
    return src;
}

/* convert template into string */
char* tpltostr(char* str) {
    
    char* proper[10];
    char* prop[10];
    int i = 0;
    char* p = "$";
    char s[128];
    char ss[128];
    char* tmp = strdup(str);
    
    while((strchr(str, '$')) != NULL) {
        /* obtain every property whose format is "$PROPERTY$" */
        sscanf(str,"%*[^$]$%[^$]",s);
        proper[i] = s;
        
        /* convert property into value */
        if( (strcmp(proper[i], "HOSTNAME")) == 0 ) {
            char hostname[128];
            gethostname(hostname,sizeof(hostname));
            proper[i] = hostname;
        }else {
            printf("not support this property!!\n");
        }
    
        strcpy(ss, s);
        char* propt = (char*)malloc(128);
        memset(propt, '\0',128);
        propt[0]='$';
        strcat(ss, p);
        strcat(propt,ss);
        prop[i] = strdup(propt);
        
        free(propt);
        propt = NULL;
        
        char* pos1 = strstr(str, ss);
        pos1 += strlen(ss);
        str = pos1;
        
        i++;
    }
    
    int j = 0;
    char* new;
    
    for( j; j< i; j++) {
        new = strreplace(tmp, prop[j], proper[j]);
        tmp = new;
    }

    int k = 0;
    for ( k; k < i; k++ )
        free(prop[k]);

    return new;
}

int main() {
    char* str = (char*)malloc(512);
    str = "/rsyslog.$HOSTNAME$.$USER$.log";
    char* string = tpltostr(str);
    printf("%s\n", string);
    free(string);
    return 0;
}
