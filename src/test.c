#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <fcntl.h>  
#include <pthread.h>
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>


void main()
{

    struct sockaddr_in dest_address;
	int sockfd = 0,i=0;

    while(i < 20)
    {

    	sockfd = socket(AF_INET,SOCK_STREAM,0);
		fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL, 0) | O_NONBLOCK);
        dest_address.sin_family= AF_INET;
        dest_address.sin_port=htons(7096);
        dest_address.sin_addr.s_addr=inet_addr("172.16.107.65");
        memset(&(dest_address.sin_zero),'\0',8);

	    connect(sockfd,(struct sockaddr *)&dest_address,sizeof(struct sockaddr));
 connect(sockfd,(struct sockaddr *)&dest_address,sizeof(struct sockaddr));

		close(sockfd);
       	sleep(10);
    }
}



