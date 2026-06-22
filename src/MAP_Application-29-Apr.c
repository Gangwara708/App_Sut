/******************************************************************************
 ** FUNCTION      :	Contains the source code for TEP 
 *******************************************************************************
 ** FILE          : MT_SMS_Delivery_ini.c
 **
 ** DESCRIPTION   : Emulates GMSC side of MT SMS Delivery
 **
 ** Copyright (c) 2009, Aricent Technologies Ltd.
 ******************************************************************************/ 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include "app_map_user.h"
#include <netdb.h>  
#include <pthread.h>
#include <unistd.h>
#include <signal.h>

#if APPL_OS_LINUX
#include <unistd.h>
#endif

#include "mp_ser_dlg_api.h"
#include "mp_ser_api_define.h"

#define NUM_READ_FDS 30  
#define API_HDR_SIZE 5

unsigned char			msc_user_id = 8;
unsigned char			user_id = 8;
unsigned char			vlr_user_id = 7;
unsigned char			gsmscf_user_id = 1;
unsigned char			hlr_user_id = 6;


int give_trace = 0;

#define SS7P_MAX_IPADDR_LEN 300
#define MAP_NUMBER_OF_HUNG_TCAP_DIALOGS 40000
#define SMSC_SSN			8
#define MSC_SSN			8
#define GMSC_SSN			8
#define HLR_SSN				6	

unsigned long open_req_c=0, srism_v3_req_c = 0,srism_v3_cnf_c = 0,mtfsm_v3_req_c = 0, mtfsm_v3_cnf_c = 0, close_ind_c = 0,gb_msg_sent;

int user_read_port;        /*Port were the wrapper sets up its server. It is
							 using this port, the wrapper will read the data 
							 from the user.	*/

int tc_user_server_port;  /*Port were the user sets up its server. 
							A client connetion has to be made to this 
							port by the wrapper. */ 
unsigned char* user_ip_addr; /* The address of the Machine where the User 
								is running.*/

unsigned char origSSN = SMSC_SSN;
unsigned char g_sap = 1;
unsigned int g_spc = 3009;
unsigned int g_dpc = 2008;

fd_set  map_readfd;
fd_set  map_writefd;
fd_set  map_exceptfd;
int 	sceFilePrsnt = 0;
int     num_fd;
int     num_selected_fd; 
struct  timeval  tmap_user_select_timeout,print_time; 
struct  timeval  ss7p_suggested_select_timeout; 
struct  timeval  min_select_timeout;
int    	dummy = 0; 
int 	is_pumping_allowed = 0;

#define MAX_LINE_BYTE 1000
#define APP_MAP_MAX_FILE_LENGTH 100
#define MAX_APIS 100

typedef struct app_map_scenario{

//	int number_apis;
	//char api_name[MAX_APIS][APP_MAP_MAX_FILE_LENGTH];
	char api_name[APP_MAP_MAX_FILE_LENGTH];
	int  num_of_messages;
	int timer_duration; 
	
}app_map_scenarios_t;

app_map_scenarios_t *p_app_map_apis=NULL;


typedef struct {
		unsigned short int        instance_id;
		unsigned char     tier;
		unsigned short int        layer_cont;
		unsigned char         variant;
		unsigned char         bep_variant;
		unsigned char         port_trace;
		unsigned char         system_trace;
		unsigned short int        entity_id;
} app_self_info_t;

extern   app_self_info_t app_map_self;

int user_sock_read_fd; // Used to receive msges from user
int user_sock_write_fd; // Used to send msgs to user
int listen_fd;		

struct timeval current_time,expiry_time, select_timer;
struct timeval my_timeout; 


void fill_api_header(map_api_header_t *head, U8bit user_id);
void fillOrigAdd(map_open_request_t *open);
void fill_open_req(map_open_request_t *open,unsigned int);  
void fill_open_req_without_delimiter_flag (map_open_request_t *open,unsigned int);

void fillAcn( map_open_request_t *open, map_appl_context_t appl_cntxt, map_appl_version_t version );
void consDestAdd(map_open_request_t *open, U32bit a_ssn);
void fill_header(map_service_header_t *header, U32bit corr_id);
void deregister_user(void);
extern map_correlation_id_t app_map_get_new_correlation_id(void);

void send_srism_v3_req(void);
void fill_srism_v3_req_arg(map_routing_info_for_sm_request_t *sri_for_sm_req);
void send_open_req(void);

void send_mtfsm_v3_req(void);
void fill_mtfsm_v3_req_arg(map_mt_forward_sm_request_t *mt_forw_sm_req);
void print_values(int);

int send_counter = 1;
unsigned int sleep_timer = 1000000;
int pumping_rate = 1;
int no_of_mesg_send = -1;
int user_registered = 0;
int msg_to_send = 0;
int choice = 0;
int conf_recv = 0;
int aborts = 0;
int uaborts = 0;
int paborts = 0;
int saborts = 0;
int dummy_flg =1;



typedef struct HexMap
{
    char chr;
    int value;
}UserHexMap;

const int HexMapL = 16;
UserHexMap HexMap[16]=
{
    {'0', 0},
    {'1', 1},
    {'2', 2},
    {'3', 3},
    {'4', 4},
    {'5', 5},
    {'6', 6},
    {'7', 7},
    {'8', 8},
    {'9', 9},
    {'A', 10},
    {'B', 11},
    {'C', 12},
    {'D', 13},
    {'E', 14},
    {'F', 15}
};

int hstoi(char *value)
{
  char *s = malloc(10) ;
  int result = 0;
  int firsttime = 1;
  int found = 0;
  int i = 0;

  strcpy(s,value);
  if (*s == '0' && *(s + 1) == 'x') s += 2;

  while (*s != '\0')
  {
    for (i = 0; i < HexMapL; i++)
    {
      if (*s == HexMap[i].chr)
      {
        if (!firsttime) result <<= 4;
        result |= HexMap[i].value;
        found = 1;
        break;
      }
    }
    if (!found) break;
    s++;
    firsttime = 0;
  }
  return result;
}



void sigusr1_hdlr(int sig)
{
		struct  timeval    l_cur_time;

		signal (SIGUSR1, sigusr1_hdlr);

		fflush(stdout); fflush(stdin);

		#if 0
		printf("\nEnter number of mesgs to be sent in one burst : ");
		scanf("%d", &pumping_rate);

		printf("\nEnter new sleep interval in Milli Sec: ");
		scanf("%d", &sleep_timer);
		sleep_timer = sleep_timer*1000;

		printf("\nTotal number of messages to send : ");
		scanf("%d", &no_of_mesg_send);

		printf("\nNew Rate [%d] Sleep Interval [%d] Msgs to Send [%d]\n",
						pumping_rate, sleep_timer, no_of_mesg_send);

		send_counter=1;
		msg_to_send = 1;
		conf_recv = 0;
		aborts = 0;
		choice = 1;
		dummy_flg = 1;


		gettimeofday(&l_cur_time, 0);
		printf("\nSignal Recv Time Sec [%d] Micro [%d]\n",l_cur_time.tv_sec,l_cur_time.tv_usec);


		#endif

		choice = 1;
		msg_to_send = 1;
		return;
}



void read_scenario()
{
	FILE *fp = NULL,*fp_apis =NULL;
	struct  timeval    l_cur_time;
	char bitmap_array[20];
	char *token_api_name=NULL,*token_num_message=NULL,*token_timer_duration =NULL,*token_length=NULL,*token_val=NULL,*token_generic = NULL;
	unsigned char line_val[MAX_LINE_BYTE];
	int max_scenarios = -1,line_count = 0,counter = 0,corr_id = 1,index =0,error = 0,buffer_val = 0;
	map_api_struct_t                   *send_api = NULL,*send_open_req_api; 
	//map_mo_forward_sm_request_t	*p_v1_fsm_req = NULL;
	map_open_request_t                 *open_req = NULL;

	signal (SIGUSR1,sigusr1_hdlr);
	fflush(stdout); fflush(stdin);

	memset(bitmap_array,'N',20);

	printf("Going to read app_map_user.conf file\n");

	//Open Configuration File


	fp = fopen("../conf/app_map_user.conf","r");
	if(fp == NULL)
	{
		printf("Unable to open the app_map_user.conf file hence exiting \n");
		exit(1);
	}
	else{
		while(1)
		{
			if (!fgets(line_val, MAX_LINE_BYTE, fp)) {

				printf("app_map_user.conf is successfully Parsed\n\n");
				break;
			}

			if ((*line_val == ' ') ||
					(*line_val == '#') ||
					(*line_val == '\n')) {
				continue;
			}
			else if((*line_val == '[') ) /*Start section */
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp)) {
					printf("app_map_user.conf is successfully Parsed\n\n");
					break;
	
				}
				else{
					printf("Reading Number of scenarios\n");
					max_scenarios = atoi(line_val);
					if(p_app_map_apis != NULL)
					{
						free(p_app_map_apis);
					}
					else{

						p_app_map_apis = malloc(sizeof(app_map_scenarios_t) * max_scenarios);	
					}

				}
			}
			else{
					token_api_name = strtok(line_val," ");
					token_num_message = strtok(NULL," ");
					token_timer_duration = strtok(NULL," ");

					strcpy(p_app_map_apis[line_count].api_name,token_api_name);
					p_app_map_apis[line_count].num_of_messages=atoi(token_num_message);
					p_app_map_apis[line_count].timer_duration=atoi(token_timer_duration);
					
					line_count++;
				
			}

		}
	}

	fclose(fp);

	//Process the SCENARIOS Let us assume File name is same as API Name


	for(counter = 0;counter <line_count ;counter++ )
	{

		send_api = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
		map_memzero(send_api,sizeof(map_api_struct_t));
		open_req = (map_open_request_t *)\
  			    app_mem_get(sizeof(map_open_request_t));
		map_memzero(open_req,sizeof(map_open_request_t));

		send_open_req_api = (map_api_struct_t *)\
				    app_mem_get(sizeof(map_api_struct_t));
		map_memzero(send_open_req_api,sizeof(map_api_struct_t));

		//fp_apis = fopen(p_app_map_apis[counter].api_name,"r");
		fp_apis = fopen("../buffers/MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST.txt","r");
		corr_id = app_map_get_new_correlation_id(); /* Get Correlation Id */

		//fill_api_header(&(send_api->header), user_id);

		printf("Coming Here\n");
		if(!strncmp(p_app_map_apis[counter].api_name,"MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST")))	
		{
			index = 0;

			map_mo_forward_sm_request_t	*p_v1_fsm_req = NULL;

			fill_api_header(&(send_open_req_api->header), user_id);

			fillOrigAdd(open_req);
			open_req->orig_add.ssn=SMSC_SSN;


			fill_open_req(open_req,corr_id);
			open_req->corr_id = corr_id;
			fillAcn(open_req, MAP_AC_SHORT_MSG_RELAY,3);
			consDestAdd(open_req, SMSC_SSN);

			//Sending OPEN Request
			send_open_req_api->header.api_id = MAP_OPEN_REQUEST;
			send_open_req_api->header.spare1 = g_sap;
			send_open_req_api->header.len = sizeof(map_open_request_t);
			send_open_req_api->header.ver = 3; 
			send_open_req_api->p_data = open_req;
			app_map_send_to_app_map((unsigned char *)send_open_req_api, &error);


			
			printf("API Name Maches MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST \n");
			fill_api_header(&(send_api->header), user_id);
			send_api->header.api_id = MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 3;
			send_api->header.len = sizeof(map_mo_forward_sm_request_t);
	

			p_v1_fsm_req = (map_mo_forward_sm_request_t*)app_mem_get(sizeof(map_mo_forward_sm_request_t)); 
			map_memzero(p_v1_fsm_req, sizeof(map_mo_forward_sm_request_t));

			printf(" MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST \n");
			fill_header(&p_v1_fsm_req->header,corr_id); 


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			strncpy(bitmap_array,line_val,20);



			
			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					if(bitmap_array[index] == '1')
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

						printf("Token Length [%s] \n",token_length);
						printf("Token Value [%s] \n",token_val);
						if(token_val == NULL)
							continue;

						for(counter = 0;counter < atoi(token_length);counter++)
						{
							if(counter == 0)
							{
								token_generic = strtok(token_val,",");
							}
							else{

								token_generic = strtok(NULL,",");
							}


							if(index == 0)
							{
								printf("Token _Generic Index 0 Choice [%s]\n",token_generic);
								//p_v1_fsm_req->arg.sm_rp_da.choice = atoi(token_generic);
								//buffer_val = hstoi(token_generic);
								p_v1_fsm_req->arg.sm_rp_da.choice = atoi(token_generic);
							}
							else if(index == 1)
							{
								if(p_v1_fsm_req->arg.sm_rp_da.choice == 1)
								{
									
									printf("Token _Generic Index 1 Length [%s]\n",token_length);
									//p_v1_fsm_req->arg.sm_rp_da.u.imsi.length = atoi(token_length);
									//p_v1_fsm_req->arg.sm_rp_da.u.imsi.value[counter] =  atoi(token_generic);
									//buffer_val = hstoi(token_length);
									p_v1_fsm_req->arg.sm_rp_da.u.imsi.length = atoi(token_length);
									buffer_val = hstoi(token_generic);
									p_v1_fsm_req->arg.sm_rp_da.u.imsi.value[counter] = buffer_val;

									printf("Token _Generic Index 1 value [%s]\n",token_generic);
								}
								else if(p_v1_fsm_req->arg.sm_rp_da.choice == 2)
								{

								}
								else if(p_v1_fsm_req->arg.sm_rp_da.choice == 3)
								{

								}
								else if(p_v1_fsm_req->arg.sm_rp_da.choice == 4)
								{

								}
								else{

									printf("Not a valid Choice hence exiting\n");
									exit(1);
								}


							}
							else if(index == 2)
							{
								//buffer_val = hstoi(token_generic);
								//p_v1_fsm_req->arg.sm_rp_oa.choice = buffer_val;
								
								p_v1_fsm_req->arg.sm_rp_oa.choice = atoi(token_generic);

							}
							else if(index == 3)
							{
								if(p_v1_fsm_req->arg.sm_rp_oa.choice == 1)
								{
									
								}
								else if(p_v1_fsm_req->arg.sm_rp_oa.choice == 2)
								{
									//p_v1_fsm_req->arg.sm_rp_oa.u.service_centre_address.length=atoi(token_length);
									//p_v1_fsm_req->arg.sm_rp_oa.u.service_centre_address.value[counter]=atoi(token_generic);
									//buffer_val = hstoi(token_length);
									p_v1_fsm_req->arg.sm_rp_oa.u.service_centre_address_oa.length = atoi(token_length);
									buffer_val = hstoi(token_generic);
									p_v1_fsm_req->arg.sm_rp_oa.u.service_centre_address_oa.value[counter] = buffer_val;

								}
								else{
									printf("Not a valid Choice hence exiting\n");

									exit(1);
								}

							}
							#if 0
							else if(index == 4)
							{
							//	p_v1_fsm_req->arg.sm_rp_ui.length = atoi(token_length);	
							//	p_v1_fsm_req->arg.sm_rp_ui.value[counter]=atoi(token_generic);


								//buffer_val = hstoi(token_length);
								p_v1_fsm_req->arg.sm_rp_ui.length = atoi(token_length);
								buffer_val = hstoi(token_generic);
								p_v1_fsm_req->arg.sm_rp_ui.value[counter] = buffer_val;

							}
							#endif
						}
					index++;
					}
					else{

						//Fill 0 Values
							
					}

				}

			}

			
			if(1)
			{
			p_v1_fsm_req->arg.sm_rp_ui.length  = 15;
			strncpy(p_v1_fsm_req->arg.sm_rp_ui.value,"Happy Diwali   ",p_v1_fsm_req->arg.sm_rp_ui.length); 
			
			p_v1_fsm_req->arg.is_extension = 1;
			p_v1_fsm_req->arg.extension_container.map_pvt_ext_count = 1;

			p_v1_fsm_req->arg.extension_container.private_ext_list[0].ext_obj_id.length = 0x03; 
			p_v1_fsm_req->arg.extension_container.private_ext_list[0].ext_obj_id.value[0] = 0x01;
			p_v1_fsm_req->arg.extension_container.private_ext_list[0].ext_obj_id.value[1] = 0x01;
			p_v1_fsm_req->arg.extension_container.private_ext_list[0].ext_obj_id.value[2] = 0xAA;

			p_v1_fsm_req->arg.extension_container.private_ext_list[0].is_ext_type = 1;

			p_v1_fsm_req->arg.extension_container.private_ext_list[0].enc_len = 3; 
			p_v1_fsm_req->arg.extension_container.private_ext_list[0].encoded[0] = 0x01;
			p_v1_fsm_req->arg.extension_container.private_ext_list[0].encoded[1] = 0x01;
			p_v1_fsm_req->arg.extension_container.private_ext_list[0].encoded[2] = 0x0BB;
			
			p_v1_fsm_req->arg.is_imsi =  MAP_FALSE;

			}

			send_api->p_data = p_v1_fsm_req;
			app_map_send_to_app_map((unsigned char *)send_api, &error);
				

		}
		else if(!strncmp(p_app_map_apis[counter].api_name,"MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST")))
		{

			index = 0;  //Making index to 0 for next processing
			
			map_mt_forward_sm_request_t	*p_mt_fsm_req = NULL;

			p_mt_fsm_req = (map_mt_forward_sm_request_t*)app_mem_get(sizeof(map_mt_forward_sm_request_t));
			map_memzero(p_mt_fsm_req,sizeof(map_mt_forward_sm_request_t));

			
			fill_api_header(&(send_open_req_api->header), user_id);
			fillOrigAdd(open_req);
			open_req->orig_add.ssn=SMSC_SSN;

			fill_open_req(open_req,corr_id);
			open_req->corr_id = corr_id;
			fillAcn(open_req, MAP_AC_SHORT_MSG_RELAY,3);
			consDestAdd(open_req, SMSC_SSN);
			open_req->dest_add.spc = g_dpc;


			//Sending OPEN Request
			send_open_req_api->header.api_id = MAP_OPEN_REQUEST;
			send_open_req_api->header.spare1 = g_sap;
			send_open_req_api->header.len = sizeof(map_open_request_t);
			send_open_req_api->header.ver = 3; 
			send_open_req_api->p_data = open_req;
			app_map_send_to_app_map((unsigned char *)send_open_req_api, &error);


			printf("API Name Matches MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST \n");
			fill_api_header(&(send_api->header), user_id);
			send_api->header.api_id = MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 3;
			send_api->header.len = sizeof(map_mt_forward_sm_request_t);

			fill_header(&p_mt_fsm_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);



			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					if(bitmap_array[index] == '1')
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

						printf("Token Length [%s] \n",token_length);
						printf("Token Value [%s] \n",token_val);
						if(token_val == NULL)
							continue;

						for(counter = 0;counter < atoi(token_length);counter++)
						{
							if(counter == 0)
							{
								token_generic = strtok(token_val,",");
							}
							else{

								token_generic = strtok(NULL,",");
							}


							if(index == 0)
							{
								//buffer_val = hstoi(token_generic);	
								p_mt_fsm_req->arg.sm_rp_da.choice = atoi(token_generic); 	

							}
							else if( index == 1)
							{

							    if(p_mt_fsm_req->arg.sm_rp_da.choice == 1)	
							    {	
								//buffer_val = hstoi(token_length);
								p_mt_fsm_req->arg.sm_rp_da.u.imsi.length =  atoi(token_length);
								buffer_val = hstoi(token_generic);
								p_mt_fsm_req->arg.sm_rp_da.u.imsi.value[counter] = buffer_val;

							    }
							    else if(p_mt_fsm_req->arg.sm_rp_da.choice == 2)
							    {

							    }
							    else if(p_mt_fsm_req->arg.sm_rp_da.choice == 3)
							    {
			
							    }
							    else{
								printf("Unknown Choice Hence Exiting \n");
								exit(1);
							    }
							}
							else if(index == 2)
							{
								//buffer_val = hstoi(token_generic);
								p_mt_fsm_req->arg.sm_rp_oa.choice = atoi(token_generic);
							}
							else if(index == 3)
							{
								if(p_mt_fsm_req->arg.sm_rp_oa.choice == 1)
								{

								}
								else if(p_mt_fsm_req->arg.sm_rp_oa.choice == 2)	
								{
									//buffer_val = hstoi(token_length);
									p_mt_fsm_req->arg.sm_rp_oa.u.service_centre_address_oa.length =  atoi(token_length);

									buffer_val = hstoi(token_generic);
									p_mt_fsm_req->arg.sm_rp_oa.u.service_centre_address_oa.value[counter] = buffer_val;
								
		
								}
								else{

									printf("Unknown Choice Hence Exiting \n");
									exit(1);
								}
							}
							else if(index == 4) //ui	
							{
								p_mt_fsm_req->arg.sm_rp_ui.length = strlen(token_generic);
								strncpy(p_mt_fsm_req->arg.sm_rp_ui.value,token_generic,strlen(token_generic));
									
							}
							else if(index == 5)
							{
								
							}
						

						}

						index++;
					 }
					 else{
						fgets(line_val, MAX_LINE_BYTE, fp_apis);

						index++;
						if(index == 1)
						{

						}
						else if(index == 2)
						{

						}
						else if(index == 3)
						{

						}
						else if(index == 4)
						{

						}
						else if(index == 5)
						{
							p_mt_fsm_req->arg.is_more_messages_to_send = 0;
							index++;

						}
						else if(index == 6)
						{

						}
						else if(index == 7)
						{
							p_mt_fsm_req->arg.is_extension = 0;
							index++;
						}

						

					 }

				}
	
			}


			send_api->p_data = p_mt_fsm_req;
			app_map_send_to_app_map((unsigned char *)send_api, &error);


		}
		else if(!strncmp(p_app_map_apis[counter].api_name,"MAP_V1_ALERT_SERVICE_CENTRE_REQUEST",strlen("MAP_V1_ALERT_SERVICE_CENTRE_REQUEST")))
		{

			index = 0;
			map_v1_alert_service_centre_without_result_request_t *p_alert_ser_centre_req = NULL;
			p_alert_ser_centre_req = (map_v1_alert_service_centre_without_result_request_t *)\
						 app_mem_get (sizeof (map_v1_alert_service_centre_without_result_request_t));
			map_memzero(p_alert_ser_centre_req, sizeof (map_v1_alert_service_centre_without_result_request_t));

	
			fill_api_header(&(send_open_req_api->header), hlr_user_id);
			fillOrigAdd(open_req);
			open_req->orig_add.ssn=HLR_SSN;

			fill_open_req(open_req,corr_id);
			open_req->corr_id = corr_id;
			fillAcn(open_req, MAP_AC_SHORT_MSG_ALERT,1);
			consDestAdd(open_req, MSC_SSN);
			open_req->dest_add.spc = g_dpc;


			//Sending OPEN Request
			send_open_req_api->header.api_id = MAP_OPEN_REQUEST;
			send_open_req_api->header.spare1 = g_sap;
			send_open_req_api->header.len = sizeof(map_open_request_t);
			send_open_req_api->header.ver = 1; 
			send_open_req_api->p_data = open_req;
			app_map_send_to_app_map((unsigned char *)send_open_req_api, &error);

			
			printf("API Name Matches MAP_V1_ALERT_SERVICE_CENTRE_REQUEST \n");
			fill_api_header(&(send_api->header), hlr_user_id);
			send_api->header.api_id = MAP_ALERT_SERVICE_CENTRE_WITHOUT_RESULT_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 1;
			send_api->header.len = sizeof(map_v1_alert_service_centre_without_result_request_t);

			fill_header(&p_alert_ser_centre_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					if(bitmap_array[index] == '1')
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

						printf("Token Length [%s] \n",token_length);
						printf("Token Value [%s] \n",token_val);
						if(token_val == NULL)
							continue;

						for(counter = 0;counter < atoi(token_length);counter++)
						{
							if(counter == 0)
							{
								token_generic = strtok(token_val,",");
							}
							else{

								token_generic = strtok(NULL,",");
							}


							if(index == 0)
							{
								p_alert_ser_centre_req->arg.msisdn.length = atoi(token_length);
								buffer_val = hstoi(token_generic); 	
								p_alert_ser_centre_req->arg.msisdn.value[counter] = buffer_val;		
							}
							else if(index == 1)
							{

								p_alert_ser_centre_req->arg.service_centre_address.length = atoi(token_length);
								buffer_val = hstoi(token_generic);
								p_alert_ser_centre_req->arg.service_centre_address.value[counter] = buffer_val;
							}


						}

						index++;

					 }
					 else{
						
						printf("No Optional Parameter is there so Please check\n");
						break;
					}
					 
				}

			}


		}
		else{

			printf("Invalid API Hence continuing\n");
		}

		fclose(fp_apis);
	}

}


/*******************************************************************************
 ** FUNCTION NAME: thread_recv
 **
 ** DESCRIPTION: Receiver thread 
 **
 ** RETURNS:  void * 
 **       
 ******************************************************************************/
void *thread_recv(void * arg)
{
		int i =0;
	    signal(SIGUSR2, print_values);

		while (-1 != i)
		{
				/* This function is the map scheduler function.*/
				unsigned int ecode; 

				num_fd = 0;
				FD_ZERO((&map_readfd)); 
				FD_ZERO((&map_writefd));
				FD_ZERO((&map_exceptfd));
				if (APP_FAILURE == app_map_pre_select(&num_fd,(&map_readfd),(&map_writefd),(&map_exceptfd),&ss7p_suggested_select_timeout, &ecode)) 
				{
						printf("\n app_map_pre_select Failed \n");
						exit(0);
				}
				num_selected_fd = select (num_fd,(&map_readfd),(&map_writefd),(&map_exceptfd),&ss7p_suggested_select_timeout);
				if (APP_FAILURE == app_map_schedule(num_selected_fd,(&map_readfd),(&map_writefd),(&map_exceptfd), &min_select_timeout, &ecode))
				{
						printf("\n app_map_schedule Failed \n");
						exit(0);
				}

		}
}

void print_values(int sig)
{
	signal(SIGUSR2, print_values);
	fflush(stdout);

	printf(" Open requests sent :: %d\n Close Indication received :: %d\n MTFSM Requests sent :: %d\n MTFSM Confirms received :: %d\n\n", open_req_c, close_ind_c, mtfsm_v3_req_c, mtfsm_v3_cnf_c);
	fflush(stdout);
	return;
}


/*******************************************************************************
 ** FUNCTION NAME: register_user
 **
 ** DESCRIPTION:  register the user
 **
 ** RETURNS:  void 
 **       
 ******************************************************************************/
void register_user(int l_ssn,int l_user_id)
{
		int 								error = 0;
		map_api_struct_t        			*p_send_api;
		map_app_register_user_request_t     *reg_req;
		p_send_api = app_mem_get(sizeof(map_api_struct_t));
		map_memzero(p_send_api,sizeof(map_api_struct_t));	

		p_send_api->header.api_id = 0;
		p_send_api->header.src_id = 0; /* TEMP */
		p_send_api->header.dst_id = 0; /* TEMP */
		p_send_api->header.ver = 1;
		p_send_api->header.user_id = l_user_id;
		p_send_api->header.len = 0;
		p_send_api->header.spare1 = g_sap; /* TEMP */
		/* send register request */
		reg_req = (map_app_register_user_request_t*)\
				app_mem_get(sizeof(map_app_register_user_request_t));

		map_memzero(reg_req, sizeof(map_app_register_user_request_t));

		reg_req->user_id = l_user_id;
		reg_req->ssn = l_ssn;
		reg_req->is_supported_ac_list = 0;
		reg_req->supported_ac_list[1] = 1;
		reg_req->sap_id = g_sap;
		reg_req->spc = g_spc;

		p_send_api->header.api_id = MAP_REGISTER_USER_REQUEST;
		p_send_api->header.len = sizeof(map_app_register_user_request_t);
		p_send_api->p_data = reg_req;

		app_map_send_to_app_map((unsigned char *)p_send_api, &error);
		printf("Sent MAP_REGISTER_USER_REQUEST for SSN %d\n",reg_req->ssn);
}

/*******************************************************************************
 ** FUNCTION NAME: thread_send
 **
 ** DESCRIPTION: invokes send_check_imei_req 
 **
 ** RETURNS:  void 
 **       
 ******************************************************************************/
void thread_send(void )
{
	signal (SIGUSR1, sigusr1_hdlr);
    	signal(SIGUSR2, print_values);

	while(1)
	{

		/*Register The User */
		if((user_registered == 0) && (choice == 1))
		{

			register_user(SMSC_SSN,msc_user_id);
			register_user(HLR_SSN,hlr_user_id);
			choice = 0;
			sleep(4);
		}
		/*User Registered */


		if(msg_to_send == 1)
		{
			read_scenario();
			msg_to_send = 0;
		}
	}/*while 1*/
}


/*******************************************************************************
 ** FUNCTION NAME: main
 **
 ** DESCRIPTION: main function
 **
 ** RETURNS:  int 
 **       
 ******************************************************************************/
int main(int argc, char *argv[])
{
		unsigned int 	i;
		unsigned int 	tier = 0; 
		unsigned int 	inst_id = 0; 
		unsigned char 	a_em [SS7P_MAX_IPADDR_LEN + 100] = {0};
		unsigned char 	s_em [SS7P_MAX_IPADDR_LEN + 100] = {0};
		unsigned char 	m_ip [SS7P_MAX_IPADDR_LEN] = {0};
		unsigned char 	a_em_ip [SS7P_MAX_IPADDR_LEN] = {0};
		unsigned char 	s_em_ip [SS7P_MAX_IPADDR_LEN] = {0};
		unsigned int 	a_em_port = 0; 
		unsigned int 	s_em_port = 0; 
		unsigned char* 	temp;
		unsigned char 	t_flag,i_flag,a_flag,s_flag,m_flag,v_flag, options_flag;
		unsigned char 	a_em_complete,s_em_complete; 
		unsigned int  err; 
		char*   p_current_dir = NULL; 

		pthread_attr_t  attr;
		pthread_t         recvThrId;

		int proc_pid;
		
		signal (SIGUSR2, print_values);
		signal (SIGUSR1, sigusr1_hdlr);

		i = 1;	
		printf("\nWRAPPER START:\n");
		//proc_pid = getpid();
		//printf("PID proc_pid %d", (int) proc_pid);
		//getchar();



		my_timeout.tv_sec = 1;
		my_timeout.tv_usec = 1000;

		t_flag=i_flag = a_flag = s_flag = m_flag = v_flag = options_flag = APP_FALSE; 
		a_em_complete = s_em_complete = APP_FALSE;
		if ( (p_current_dir = (char *)getenv("PWD")) == NULL)   
		{
				printf("EM :: Failed to get the ENVIRONMENT VARIABLE PWD\n");
				exit(0);
		}



		while((i = getopt (argc, argv, "ht:i:a:s:m:f:v:")) != -1)
		{
				options_flag = APP_TRUE; 
				switch(i)
				{
						case 'f' :

								break;
						case 't' :   /* tier */
								tier = atoi (optarg);
								t_flag = APP_TRUE; 
								break;

						case 'i' :   /* instance id */
								inst_id = atoi (optarg);
								i_flag = APP_TRUE; 
								break;

						case 'a' :   /* active em info */
								a_flag = APP_TRUE; 
								a_em_complete = APP_TRUE; 
								strcpy(a_em, optarg);
								temp = (unsigned char *) strtok (a_em, ":"); 
								if (temp != NULL )
								{
										strcpy(a_em_ip, temp); 
								}
								else
								{
										a_em_complete = APP_FALSE; 
								}
								temp = (unsigned char *)strtok( NULL, ":"); 
								if ( temp != NULL )
								{
										a_em_port = atoi(temp); 
								}
								else
								{
										a_em_complete = APP_FALSE; 
								}
								break;

						case 's' :   /* standby em info */
								s_flag = APP_TRUE; 
								s_em_complete = APP_TRUE ; 
								strcpy(s_em, optarg);
								temp = (unsigned char *)strtok (s_em, ":"); 
								if (temp != NULL )
								{
										strcpy(s_em_ip, temp); 
								}
								else
								{   
										s_em_complete = APP_FALSE; 
								}
								temp = (unsigned char *)strtok( NULL, ":"); 
								if ( temp != NULL )
								{
										s_em_port = atoi(temp); 
								}
								else
								{
										s_em_complete = APP_FALSE; 
								}
								break;
						case 'm' :   /* machine's IP address */
								m_flag = APP_TRUE;
								strcpy(m_ip, optarg);
								break;
						case 'h' :       /* fall thru */
						default :
								printf("\n\nUsage: AppUser -t tier -i inst_id -a ip:port "
												"[-s ip:port] [-m ip] \n\n"); 
								printf("-h\thelp\n");   
								printf("-t\ttier level of SS7 platform process\n");
								printf("-i\tinstance id in the tier\n");
								printf("-a\tip address & port of active em\n");
								printf("-s\tip address & port of standby em\n");
								printf("-m\tmachines's ip address on which this process"
												" runs\n");

								exit(1);
				}
		}


		/* checking for the inputs */
		if ( options_flag == APP_FALSE)
		{
				printf("\n\nUsage: TcUser -t tier -i inst_id -a ip:port "
								"[-s ip:port] [-m ip] \n\n"); 
				printf("-h\thelp\n");   
				printf("-t\ttier level of SS7 platform process\n");
				printf("-i\tinstance id in the tier\n");
				printf("-a\tip address & port of active em\n");
				printf("-s\tip address & port of standby em\n");
				printf("-m\tmachines's ip address on which this process"
								" runs\n");

				exit(1);
		}

		if ( t_flag == APP_FALSE )
		{
				printf("SS7P_PORT:: ERROR: tier is not specified \n");
				exit(1);
		}

		if ( i_flag == APP_FALSE )
		{
				printf("SS7P_PORT:: ERROR: instance_id is not specified \n");
				exit(1);
				return 0;
		}

		if ( a_flag == APP_FALSE )
		{
				printf("SS7P_PORT:: ERROR: active_em ip:port is not specified \n");
				exit(1);
				return 0;
		}
		else
		{
				if ( a_em_complete == APP_FALSE )
				{
						printf("SS7P_PORT:: ERROR: active_em ip:port is not specified \n");
						exit(1);
				}
		}

		if ( s_flag == APP_FALSE )
		{
				/* do nothing as this parameter is optional */
		}
		else
		{
				if ( s_em_complete == APP_FALSE )
				{
						printf("SS7P_PORT:: ERROR: standby_em ip:port is not specified \n");
						exit(1);
				}
		}
		/* initialize MAP application platform library */
		if (app_map_init(tier,inst_id,a_em_ip,a_em_port,s_em_ip,s_em_port,m_ip, APPL_IPC_OPT_USE_SELECT,p_current_dir,&err) == 0)
		{
				printf("Could not initialise TCAP \n"); 
				exit(3);
		}

		i = 0; 
		app_map_self.port_trace = 0;
		pthread_attr_init(&attr);
		pthread_attr_setscope(&attr,PTHREAD_SCOPE_SYSTEM);
		pthread_create(&recvThrId, &attr, (void*)thread_recv, NULL);
		pthread_attr_destroy(&attr);

		sleep_timer = 1000000;	
		expiry_time.tv_sec = 0;	
		expiry_time.tv_usec = 0;	

		while (-1 != i)
		{
				/* This function is the tmap scheduler function.*/
				if (gettimeofday(&current_time,NULL) == 0)
				{
						if (current_time.tv_sec > expiry_time.tv_sec || ( current_time.tv_sec == expiry_time.tv_sec && current_time.tv_usec >= expiry_time.tv_usec))
						{
								thread_send();
								/*		gettimeofday(&current_time,NULL);*/
								expiry_time.tv_sec =    current_time.tv_sec
										+ (current_time.tv_usec + sleep_timer)/1000000;
								expiry_time.tv_usec =   (current_time.tv_usec + sleep_timer)%1000000;
						}
						else
						{
								usleep(((expiry_time.tv_sec*1000000) + expiry_time.tv_usec)  - (( current_time.tv_sec*1000000) + current_time.tv_usec));
						}	
				}
		}
		return(1);
}


/*Fills the api header */
void fill_api_header(map_api_header_t *head, U8bit user_id)
{
		head->api_id = 0;
		head->src_id = 0; /* TEMP */
		head->dst_id = 0; /* TEMP */
		head->ver = 2;
		head->user_id = user_id;
		head->len = 0;
		head->spare1 = g_sap; /* TEMP */
		head->spare2 = 0;
}

/* Fills the originating address*/
void fillOrigAdd(map_open_request_t *open)
{
		open->is_orig_add =1;
		open->orig_add.routing_ind = ROUTE_ON_SSN; /* 1 for Route on SSN and 0 for GT*/
		open->orig_add.is_spc = 1;
		open->orig_add.spc = g_spc;
		open->orig_add.is_ssn = 1;
		open->orig_add.ssn = origSSN;
		open->orig_add.is_gt = 0;
		open->orig_add.global_title_ind = MAP_GT_WITH_TT_NP_ES_NAI; /*Section 3.4.1 possible values = MAP_NO_GT,MAP_GT_WIT
																	  H_NAI,MAP_GT_WITH_TT,MAP_GT_WITH_TT_NP_ES,MAP_GT_WITH_TT_NP_ES_NAI,MAP_GT_RESERVED */
		open->orig_add.gt.type4.translation_type = 0;
		open->orig_add.gt.type4.numbering_plan = 1;
		open->orig_add.gt.type4.encoding_scheme = 2;
		open->orig_add.gt.type4.nature_of_addr_ind = 4;
		open->orig_add.gt.type4.num_gt_addr_info_octets = 6;
		open->orig_add.gt.type4.gt_addr_info[0] = 0x19;
		open->orig_add.gt.type4.gt_addr_info[1] = 0x29;
		open->orig_add.gt.type4.gt_addr_info[2] = 0x02;
		open->orig_add.gt.type4.gt_addr_info[3] = 0x50;
		open->orig_add.gt.type4.gt_addr_info[4] = 0x05;
		open->orig_add.gt.type4.gt_addr_info[5] = 0x90;
}


/* Fills the open request*/
/* Fills the open request*/
void fill_open_req_without_delimiter_flag(map_open_request_t *open, unsigned int corr_id)
{
		open->corr_id = corr_id;	
		open->is_dest_ref = 0;
		open->dest_ref.length = 7;
		open->dest_ref.value[0] = 0x1;
		open->dest_ref.value[1] = 0x1;
		open->dest_ref.value[2] = 0x1;
		open->dest_ref.value[3] = 0x1;
		open->dest_ref.value[4] = 0x1;
		open->dest_ref.value[5] = 0x1;
		open->dest_ref.value[6] = 0x1;

		open->is_orig_ref = 0;
		open->orig_ref.length = 0;
		open->orig_ref.value[0] = 0x2;
		open->orig_ref.value[1] = 0x2;
		open->orig_ref.value[2] = 0x2;
		open->orig_ref.value[3] = 0x2;
		open->orig_ref.value[4] = 0x2;
		open->orig_ref.value[5] = 0x2;
		open->orig_ref.value[6] = 0x2;

		open->is_qos = 1;
		open->qos.protocol_class = MAP_SCCP_PROTOCOL_CLASS_0;
		open->qos.return_option = MAP_SCCP_RETURN_ON_ERROR;
		open->qos.sequence_control = 1;
		open->is_spec_info = MAP_FALSE;
		open->wait_for_delimiter = 0;
}


void fill_open_req(map_open_request_t *open, unsigned int corr_id)
{
		open->corr_id = corr_id;	
		open->is_dest_ref = 0;
		open->dest_ref.length = 7;
		open->dest_ref.value[0] = 0x1;
		open->dest_ref.value[1] = 0x1;
		open->dest_ref.value[2] = 0x1;
		open->dest_ref.value[3] = 0x1;
		open->dest_ref.value[4] = 0x1;
		open->dest_ref.value[5] = 0x1;
		open->dest_ref.value[6] = 0x1;

		open->is_orig_ref = 0;
		open->orig_ref.length = 0;
		open->orig_ref.value[0] = 0x2;
		open->orig_ref.value[1] = 0x2;
		open->orig_ref.value[2] = 0x2;
		open->orig_ref.value[3] = 0x2;
		open->orig_ref.value[4] = 0x2;
		open->orig_ref.value[5] = 0x2;
		open->orig_ref.value[6] = 0x2;

		open->is_qos = 1;
		open->qos.protocol_class = MAP_SCCP_PROTOCOL_CLASS_0;
		open->qos.return_option = MAP_SCCP_RETURN_ON_ERROR;
		open->qos.sequence_control = 1;
		open->is_spec_info = MAP_FALSE;
		open->wait_for_delimiter = 1;
}


/*Fills the application context */
void fillAcn( map_open_request_t *open, map_appl_context_t appl_cntxt, map_appl_version_t version )
{
		open->app_cont_name.appl_context = appl_cntxt;
		open->app_cont_name.version =  version;
		return;
}


/*Fills the destination address*/
void consDestAdd(map_open_request_t *open, U32bit a_ssn)
{
		/* Section 3.4 ITUT- Q.713 0 means Route on Gt , 1 means Route on SSN */
		open->dest_add.routing_ind = ROUTE_ON_SSN;
		open->dest_add.is_spc = 1;
		open->dest_add.spc = g_dpc;
		open->dest_add.is_ssn = 1;
		open->dest_add.ssn = a_ssn;
		open->dest_add.is_gt = 0;
		/*Section 3.4.1 possible values = MAP_NO_GT,MAP_GT_WIT H_NAI,MAP_GT_WITH_TT,MAP_GT_WITH_TT_NP_ES,MAP_GT_WITH_TT_NP_ES_NAI,MAP_GT_RESERVED*/
		open->dest_add.global_title_ind =MAP_GT_WITH_TT_NP_ES_NAI; 
		open->dest_add.gt.type4.translation_type = 0;
		open->dest_add.gt.type4.numbering_plan = 1;
		open->dest_add.gt.type4.encoding_scheme = 2;
		open->dest_add.gt.type4.nature_of_addr_ind = 4;
		open->dest_add.gt.type4.num_gt_addr_info_octets = 6;
		open->dest_add.gt.type4.gt_addr_info[0] = 0x19;
		open->dest_add.gt.type4.gt_addr_info[1] = 0x89;
		open->dest_add.gt.type4.gt_addr_info[2] = 0x68;
		open->dest_add.gt.type4.gt_addr_info[3] = 0x04;
		open->dest_add.gt.type4.gt_addr_info[4] = 0x45;
		open->dest_add.gt.type4.gt_addr_info[5] = 0x14;
}

/*Fills the service header */
void fill_header(map_service_header_t *header, U32bit corr_id)
{
		header->is_corr_id = 1;
		header->corr_id = corr_id;
		header->invoke_id = 0x01;
		header->is_linked_id = 0;
		header->last_component = 1;
}



/*******************************************************************************
 ** FUNCTION NAME: app_map_send_to_app_map_user
 **
 ** DESCRIPTION: Call back function used by the BEP to send messages to the user
 **
 ** RETURNS: 1 on success 
 **       
 ******************************************************************************/


int app_map_send_to_app_map_user (unsigned char *p_buffer, 
				unsigned short noctets)
{
		map_api_struct_t		*p_api;

		p_api = (map_api_struct_t *)p_buffer;
		switch(p_api->header.api_id)
		{
				case  MAP_APP_SAP_SSN_STATE_INDICATION:
						{
								map_app_sap_ssn_state_indication_t  *open_ind;
								open_ind = (map_app_sap_ssn_state_indication_t *)p_api->p_data;
								printf("Received SAP STATE IND state [%d] SSN [%d]SAP ID [%d]\n", open_ind->state, open_ind->ssn, open_ind->sap_id);

						}
						break;


				case MAP_REGISTER_USER_CONFIRM:
						{
								user_registered = 1;		
								printf("Received MAP_REGISTER_USER_CONFIRM from MAP stack\n");
						}
						break;

				case MAP_OPEN_CONFIRM:
						{
								map_open_confirm_t 	*p_open_conf = NULL;
								p_open_conf = (map_open_confirm_t *)p_api->p_data;
								if(give_trace)printf("Received MAP_OPEN_CONFIRM from MAP stack\n");
								/*
								   if required do some processing	
								 */	

						}
						break;



				case  MAP_SEND_ROUTING_INFO_FOR_SM_CONFIRM:

						{
								map_routing_info_for_sm_confirm_t *sri_for_sm_conf = NULL;

								sri_for_sm_conf = (map_routing_info_for_sm_confirm_t *) p_api->p_data;
								if (give_trace) printf("Received MAP_SEND_ROUTING_INFO_FOR_SM_CONFIRM from MAP stack\n");

								if(sri_for_sm_conf->choice == MAP_RESULT)
								{
										srism_v3_cnf_c++;
								}

						}
						break;

				case MAP_MT_FORWARD_SHORT_MESSAGE_CONFIRM:
						{

								map_mt_forward_sm_confirm_t *mt_fsm_conf = (map_mt_forward_sm_confirm_t *)p_api->p_data;
								mtfsm_v3_cnf_c++;
								if (give_trace) printf("Received  MAP_MT_FORWARD_SHORT_MESSAGE_CONFIRM from MAP stack\n");

						}
						break;

				case MAP_MO_FORWARD_SHORT_MESSAGE_CONFIRM:
				{

					printf("MAP_MO_FORWARD_SHORT_MESSAGE_CONFIRM API Recieved########## \n\n");
				}

				break;

				case MAP_CLOSE_INDICATION:
						{
								map_close_indication_t *p_close_ind = NULL;
								if (give_trace) printf("Received MAP_CLOSE_INDICATION from MAP stack\n");
								if (give_trace) printf("--------------------------------------------\n");
								p_close_ind = (map_close_indication_t *) p_api->p_data;
								/*
								   if required do some processing	
								 */	
								close_ind_c++;	
						}
						break;

				case MAP_N_PC_STATE_INDICATION:
						{
								map_n_pc_state_indication_t *p_n_pc_state_ind = NULL;
								p_n_pc_state_ind = (map_n_pc_state_indication_t *)p_api->p_data;
								printf("PC [%d] State [%d] \n",p_n_pc_state_ind->affected_spc,p_n_pc_state_ind->affected_spc_status);
								if (p_n_pc_state_ind->affected_spc_status == 3)
								{
										is_pumping_allowed = 1;
								}
								else
								{
										is_pumping_allowed = 0;
								}
						}
						break;

				case MAP_N_STATE_INDICATION :
						{
								map_n_state_indication_t *p_n_state_ind = NULL;
								p_n_state_ind = (map_n_state_indication_t *)p_api->p_data;
								printf("SSN [%d] State [%d] \n",p_n_state_ind->affected_subsystem.ssn,p_n_state_ind->subsys_status);
						}
						break;

				case MAP_U_ABORT_INDICATION:
						{
								uaborts++;
								if (give_trace) 	printf("Received MAP_U_ABORT_INDICATION");
						}
						break;


				case MAP_P_ABORT_INDICATION:
						{
								map_p_abort_indication_t	*p_abort;

								p_abort = (map_p_abort_indication_t *) p_api->p_data;
								if (give_trace) printf("\nP-ABORT Recd Reason [%d] SRC [%d]\n", p_abort->prov_reason,p_abort->src);
								paborts++;
						}
						break;


				case MAP_NOTICE_INDICATION:
						printf("Received MAP_NOTICE_INDICATION");
						break;

				case MAP_DELIMITER_INDICATION:
						break;

				default:
						printf("\n Unknown API encountered\n");	

		}
		app_mem_free((void *)p_api->p_data);
		app_mem_free((void *)p_api);

		if (((srism_v3_cnf_c%10 ==0) || (mtfsm_v3_cnf_c%10 ==0) ))
		{
			if( (close_ind_c) % 20 == 0)
				printf("\nOPEN_REQ [%lu] MTFSM_V3_REQ [%lu] MTFSM_V3_CNF [%lu] CLOSE_IND  [%lu] DIFF[%lu] \n", open_req_c,mtfsm_v3_req_c, mtfsm_v3_cnf_c, close_ind_c,mtfsm_v3_req_c-mtfsm_v3_cnf_c);

		}

		return APP_SUCCESS;
}




/*******************************************************************************
 ** FUNCTION NAME: deregister_user
 **
 ** DESCRIPTION: Deregisters the user from BEP
 **
 ** RETURNS:  void 
 **       
 ******************************************************************************/

void deregister_user(void)
{
		map_api_struct_t dereg_api;
		map_deregister_user_request_t *dereg_req;
		unsigned int error=APPL_ERR_NO_ERROR;

		dereg_req = (map_deregister_user_request_t*)malloc(sizeof(map_deregister_user_request_t));
		map_memzero(dereg_req, sizeof(map_deregister_user_request_t));
		fill_api_header(&dereg_api.header, msc_user_id);
		dereg_req->user_id = msc_user_id;
		dereg_api.header.api_id = MAP_DEREGISTER_USER_REQUEST;
		dereg_api.p_data = dereg_req;

		app_map_send_to_app_map((unsigned char *)&dereg_api, &error);
		printf("\nSent MAP_DEREGISTER_USER_REQUEST\n");
}	


void send_srism_v3_req(void)
{

		map_api_struct_t                   *send_api = NULL;
		map_api_struct_t                   *send_api_lu = NULL;
		map_open_request_t                 *open_req = NULL;
		int 								corr_id;
		unsigned int error = 0;
		map_routing_info_for_sm_request_t	*sri_for_sm_req = NULL;


		open_req = (map_open_request_t *)\
				app_mem_get(sizeof(map_open_request_t));
		map_memzero(open_req,sizeof(map_open_request_t));
		send_api = (map_api_struct_t *)\
				app_mem_get(sizeof(map_api_struct_t));
		map_memzero(send_api,sizeof(map_api_struct_t));
		send_api_lu = (map_api_struct_t *)\
				app_mem_get(sizeof(map_api_struct_t));
		map_memzero(send_api_lu, sizeof(map_api_struct_t));
		fill_api_header(&(send_api->header), msc_user_id);
		corr_id=app_map_get_new_correlation_id();
		fillOrigAdd(open_req);
		fill_open_req(open_req, corr_id);
		open_req->corr_id = corr_id;


		fillAcn(open_req, MAP_AC_SHORT_MSG_GATEWAY,3);
		consDestAdd(open_req, HLR_SSN);

		send_api->header.api_id = MAP_OPEN_REQUEST;
		send_api->header.spare1 = g_sap;
		send_api->header.len = sizeof(map_open_request_t);
		send_api->header.ver = 3; 
		send_api->p_data = open_req;

		app_map_send_to_app_map((unsigned char *)send_api, &error);
		open_req_c++;
		if (give_trace)printf ("\n\n-------------------------\n");
		if (give_trace)printf ("Sent MAP_OPEN_REQUEST \n ");

		sri_for_sm_req = (map_routing_info_for_sm_request_t *)\
				app_mem_get (sizeof (map_routing_info_for_sm_request_t));
		map_memzero(sri_for_sm_req,sizeof (map_routing_info_for_sm_request_t));
		fill_srism_v3_req_arg(sri_for_sm_req);


		fill_header(&(sri_for_sm_req->header), corr_id);
		fill_api_header(&(send_api_lu->header), msc_user_id);

		send_api_lu->header.api_id = MAP_SEND_ROUTING_INFO_FOR_SM_REQUEST;
		send_api_lu->header.spare1 = g_sap;
		send_api_lu->header.ver = 3; 
		send_api_lu->header.len = sizeof(map_routing_info_for_sm_request_t);
		send_api_lu->p_data = sri_for_sm_req;
		app_map_send_to_app_map((unsigned char *)send_api_lu, &error);
		srism_v3_req_c++;
		if (give_trace) printf("Sent MAP_SEND_ROUTING_INFO_FOR_SM_REQUEST V3[CID=%u]\n",corr_id);

}
void fill_srism_v3_req_arg (map_routing_info_for_sm_request_t *sri_for_sm_req)
{
		sri_for_sm_req->arg.msisdn.length = 8;
		sri_for_sm_req->arg.msisdn.value[0] = 0x13;
		sri_for_sm_req->arg.msisdn.value[1] = 0x06;
		sri_for_sm_req->arg.msisdn.value[2] = 0x01;
		sri_for_sm_req->arg.msisdn.value[3] = 0x30;
		sri_for_sm_req->arg.msisdn.value[4] = 0x20;
		sri_for_sm_req->arg.msisdn.value[5] = 0x35;
		sri_for_sm_req->arg.msisdn.value[6] = 0x18;
		sri_for_sm_req->arg.msisdn.value[7] = 0xf3;

		sri_for_sm_req->arg.sm_rp_pri = 0;

		sri_for_sm_req->arg.service_centre_address.length = 4;
		sri_for_sm_req->arg.service_centre_address.value[0] = 0x11;
		sri_for_sm_req->arg.service_centre_address.value[1] = 0x12;
		sri_for_sm_req->arg.service_centre_address.value[2] = 0x13;
		sri_for_sm_req->arg.service_centre_address.value[3] = 0x14;

		sri_for_sm_req->arg.is_extension = 0;
		sri_for_sm_req->arg.is_gprs_support_indicator = 0;
		sri_for_sm_req->arg.is_sm_rp_mti = 0;
		sri_for_sm_req->arg.is_sm_rp_smea = 0;

}

void send_open_req(void)
{

		map_api_struct_t                   *p_send_api;
		map_open_request_t                 *open_req = NULL;
		int                                corr_id ;    
		unsigned int error = 0;
		p_send_api = app_mem_get(sizeof(map_api_struct_t));
		map_memzero(p_send_api,sizeof(map_api_struct_t));
		fill_api_header(&p_send_api->header, msc_user_id);  
		open_req = (map_open_request_t *)\
				   app_mem_get(sizeof(map_open_request_t));
		map_memzero(open_req,sizeof(map_open_request_t));
		fillOrigAdd(open_req);

		open_req->orig_add.ssn=GMSC_SSN;

		fill_open_req_without_delimiter_flag(open_req,corr_id);

	/*	mt_forw_sm_req = (map_mt_forward_sm_request_t *)\
						 app_mem_get (sizeof (map_mt_forward_sm_request_t));
		map_memzero(mt_forw_sm_req,sizeof(map_mt_forward_sm_request_t));

		fill_mtfsm_v3_req_arg(mt_forw_sm_req);*/
		corr_id = app_map_get_new_correlation_id();
		//printf("Corr_id %d\n",corr_id);
		open_req->corr_id = corr_id;

		fillAcn(open_req, MAP_AC_SHORT_MSG_MT_RELAY, 3);

		consDestAdd(open_req, SMSC_SSN);
		open_req->dest_add.spc = g_dpc;

		/*  fill api header with MAP_OPEN_REQUEST api_id */
		p_send_api->header.api_id = MAP_OPEN_REQUEST;
		p_send_api->header.len = sizeof(map_open_request_t);
		/** Assign the p_data with the map_open_request_t structure **/
		p_send_api->p_data = open_req;

		app_map_send_to_app_map((unsigned char *)p_send_api, &error);
		open_req_c++;	

}
void send_mtfsm_v3_req(void)
{
		map_api_struct_t                   *p_send_api;
		map_open_request_t                 *open_req = NULL;
		map_mt_forward_sm_request_t      *mt_forw_sm_req = NULL;
		map_delimiter_request_t			*delim_req = NULL;
		int                                corr_id ;    
		unsigned int error = 0;

		p_send_api = app_mem_get(sizeof(map_api_struct_t));
		map_memzero(p_send_api,sizeof(map_api_struct_t));
		fill_api_header(&p_send_api->header, msc_user_id);    
		/* Open Rend_apiquest */
		open_req = (map_open_request_t *)\
				app_mem_get(sizeof(map_open_request_t));
		map_memzero(open_req,sizeof(map_open_request_t));
		/** Fill the Originating Address in the map open request ***/
		fillOrigAdd(open_req);

		open_req->orig_add.ssn=GMSC_SSN;

		fill_open_req(open_req,corr_id);

		mt_forw_sm_req = (map_mt_forward_sm_request_t *)\
				app_mem_get (sizeof (map_mt_forward_sm_request_t));
		map_memzero(mt_forw_sm_req,sizeof(map_mt_forward_sm_request_t));

		fill_mtfsm_v3_req_arg(mt_forw_sm_req);
		corr_id = app_map_get_new_correlation_id();
		//printf("Corr_id %d\n",corr_id);
		open_req->corr_id = corr_id;

		fillAcn(open_req, MAP_AC_SHORT_MSG_MT_RELAY, 3);

		consDestAdd(open_req, SMSC_SSN);
		open_req->dest_add.spc = g_dpc;

		/*  fill api header with MAP_OPEN_REQUEST api_id */
		p_send_api->header.api_id = MAP_OPEN_REQUEST;
		p_send_api->header.len = sizeof(map_open_request_t);
		/** Assign the p_data with the map_open_request_t structure **/
		p_send_api->p_data = open_req;

		app_map_send_to_app_map((unsigned char *)p_send_api, &error);
		if (give_trace)printf ("\n\n-------------------------\n");
		if (give_trace)printf ("Sent MAP_OPEN_REQUEST \n ");
		open_req_c++;	
		p_send_api = app_mem_get(sizeof(map_api_struct_t));
		map_memzero(p_send_api,sizeof(map_api_struct_t));
		fill_api_header(&p_send_api->header, msc_user_id);    
		fill_header(&(mt_forw_sm_req->header), corr_id);
		p_send_api->header.api_id = MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST; 
		p_send_api->header.ver = 3;
		p_send_api->header.len = sizeof(map_mt_forward_sm_request_t);
		p_send_api->p_data = mt_forw_sm_req; 

		app_map_send_to_app_map((unsigned char *)p_send_api, &error);
		if (give_trace) printf("Sent MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST V3 with AC=MAP_AC_SHORT_MSG_MT_RELAY [CID=%u]\n",corr_id);
		mtfsm_v3_req_c++;	
}

void fill_mtfsm_v3_req_arg (map_mt_forward_sm_request_t *mt_forw_sm_req)
{
		mt_forw_sm_req->arg.sm_rp_da.choice = 1;
		mt_forw_sm_req->arg.sm_rp_da.u.imsi.length = 5;
		mt_forw_sm_req->arg.sm_rp_da.u.imsi.value[0] = 0x13;
		mt_forw_sm_req->arg.sm_rp_da.u.imsi.value[1] = 0x06;
		mt_forw_sm_req->arg.sm_rp_da.u.imsi.value[2] = 0x01;
		mt_forw_sm_req->arg.sm_rp_da.u.imsi.value[3] = 0x30;
		mt_forw_sm_req->arg.sm_rp_da.u.imsi.value[4] = 0x20;

		mt_forw_sm_req->arg.sm_rp_oa.choice = 2;
		mt_forw_sm_req->arg.sm_rp_oa.u.service_centre_address_oa.length = 7;
		mt_forw_sm_req->arg.sm_rp_oa.u.service_centre_address_oa.value[0] = 0x91;
		mt_forw_sm_req->arg.sm_rp_oa.u.service_centre_address_oa.value[1] = 0x19;
		mt_forw_sm_req->arg.sm_rp_oa.u.service_centre_address_oa.value[2] = 0x89;
		mt_forw_sm_req->arg.sm_rp_oa.u.service_centre_address_oa.value[3] = 0x29;
		mt_forw_sm_req->arg.sm_rp_oa.u.service_centre_address_oa.value[4] = 0x99;
		mt_forw_sm_req->arg.sm_rp_oa.u.service_centre_address_oa.value[5] = 0x89;
		mt_forw_sm_req->arg.sm_rp_oa.u.service_centre_address_oa.value[6] = 0x18;
		mt_forw_sm_req->arg.sm_rp_ui.length = 58; /* MSG SIZE */
		strncpy(mt_forw_sm_req->arg.sm_rp_ui.value,"ABCDEFGHIJKLMNOPQRSTUVWXYZ||ABCDEFGHIJKLMNOPQRSTUVWXYZ||",mt_forw_sm_req->arg.sm_rp_ui.length); 

		mt_forw_sm_req->arg.is_more_messages_to_send = 0;


		mt_forw_sm_req->arg.is_extension = 1;
		mt_forw_sm_req->arg.extension_container.map_pvt_ext_count = 1;

		mt_forw_sm_req->arg.extension_container.private_ext_list[0].ext_obj_id.length = 0x03; 
		mt_forw_sm_req->arg.extension_container.private_ext_list[0].ext_obj_id.value[0] = 0x01;
		mt_forw_sm_req->arg.extension_container.private_ext_list[0].ext_obj_id.value[1] = 0x01;
		mt_forw_sm_req->arg.extension_container.private_ext_list[0].ext_obj_id.value[2] = 0xAA;

		mt_forw_sm_req->arg.extension_container.private_ext_list[0].is_ext_type = 1;

		mt_forw_sm_req->arg.extension_container.private_ext_list[0].enc_len = 3; 
		mt_forw_sm_req->arg.extension_container.private_ext_list[0].encoded[0] = 0x01;
		mt_forw_sm_req->arg.extension_container.private_ext_list[0].encoded[1] = 0x01;
		mt_forw_sm_req->arg.extension_container.private_ext_list[0].encoded[2] = 0x0BB;



}







