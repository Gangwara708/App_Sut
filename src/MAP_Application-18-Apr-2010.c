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

	unsigned long open_req_c=0, srism_v3_req_c = 0,srism_v3_cnf_c = 0,mtfsm_v3_req_c = 0, mtfsm_v3_cnf_c = 0, close_ind_c = 0,gb_msg_sent,open_ind_c =0,mtfsm_v3_ind_c=0,close_req_c=0;

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
	unsigned int g_spc = 2008;
	unsigned int g_dpc = 3009;

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


	//Imran Doing work Start

	typedef struct
	{
		U8bit                       	map_pvt_ext_count;
		struct
		{
			map_object_id_t         	ext_obj_id;
			map_bool_t              	is_ext_type;
			map_len_t               	enc_len;
			U8bit                   	encoded[MAP_SGSN_CAPABILITY_EXTN_MAX_LEN];
		}private_ext_list[MAP_MAX_PVT_EXTN_LIST];
	}extension_container;


	


	int init_flag = 0,index_scenarios = 0;
	int number_of_dialogues = 0;
	int max_scenarios = -1,max_scenarios_send = -1,max_scenarios_recv =-1;

	#define INVALID_ID -1
	#define MAX_SCENE_LINE_BYTE 500
	#define MAX_LINE_BYTE 1000
	#define APP_MAP_MAX_FILE_LENGTH 100
	#define MAX_APIS 100
	#define SAME_DIALOGUE 1
	#define MULTIPLE_DIALOGUE 2
	#define SAME_DIAL_MUL_MESSAGE 3
	#define MAX_DIALOGUE 100


	typedef struct app_map_apis{
		
		char api_name[APP_MAP_MAX_FILE_LENGTH];
		int total_messages;
		int count_messages;
		int timer_duration; 
	}app_map_apis_t;

	typedef struct app_map_scenario{

		int is_same_dialogue;
		int dialogue_id;
		int number_apis;
		app_map_apis_t *p_app_map_apis;
		
	}app_map_scenarios_t;

	app_map_scenarios_t *p_app_map_scenario=NULL,*p_app_map_scenario_send = NULL,*p_app_map_scenario_recv = NULL;
	//Imran Doing work end


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
	extern int map_memzero(void *,int);

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
	//void send_open_req(void);

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
			signal (SIGUSR1, sigusr1_hdlr);
			fflush(stdout); fflush(stdin);

			choice = 1;
			msg_to_send = 1;
			return;
	}



	/* Fills the originating address*/
	void fillOrignatingAdd(map_open_request_t *open,int ssn)
	{
			open->is_orig_add =1;
			open->orig_add.routing_ind = ROUTE_ON_SSN; /* 1 for Route on SSN and 0 for GT*/
			open->orig_add.is_spc = 1;
			open->orig_add.spc = g_spc;
			open->orig_add.is_ssn = 1;
			open->orig_add.ssn = ssn;
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

	void send_open_req(char *api_name ,int corr_id)
	{
		int luser_id = 0,lssn = 0,dest_ssn = 0,options = 0,version =0,error;
		map_open_request_t                 *open_req = NULL;
		map_api_struct_t 	*send_open_req_api = NULL;

		open_req = (map_open_request_t *)\
			app_mem_get(sizeof(map_open_request_t));
		map_memzero(open_req,sizeof(map_open_request_t));

		send_open_req_api = (map_api_struct_t *)\
			app_mem_get(sizeof(map_api_struct_t));
		map_memzero(send_open_req_api,sizeof(map_api_struct_t));

		
		if(!strncmp(api_name,"MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST")))
		{
			luser_id = msc_user_id;
			lssn = SMSC_SSN;
			dest_ssn = SMSC_SSN;
			options = MAP_AC_SHORT_MSG_RELAY;	
			version = 3;
		}
		else if(!strncmp(api_name,"MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST")))
		{
			luser_id = msc_user_id;
			lssn = SMSC_SSN;
			dest_ssn = SMSC_SSN;
			options = MAP_AC_SHORT_MSG_RELAY;
			version = 3;

		}
		else if(!strncmp(api_name,"MAP_V1_ALERT_SERVICE_CENTRE_REQUEST",strlen("MAP_V1_ALERT_SERVICE_CENTRE_REQUEST")))
		{
			luser_id = msc_user_id;
			lssn = HLR_SSN;
			dest_ssn = MSC_SSN;
			options = MAP_AC_SHORT_MSG_ALERT;
			version = 1;

		}
		else{

		}

		fill_api_header(&(send_open_req_api->header), luser_id);
		fillOrignatingAdd(open_req,lssn);
		fill_open_req(open_req,corr_id);	
		fillAcn(open_req, options,version);
		consDestAdd(open_req,dest_ssn);

		//Sending OPEN Request
		send_open_req_api->header.api_id = MAP_OPEN_REQUEST;
		send_open_req_api->header.spare1 = g_sap;
		send_open_req_api->header.len = sizeof(map_open_request_t);
		send_open_req_api->header.ver = version; 
		send_open_req_api->p_data = open_req;
		app_map_send_to_app_map((unsigned char *)send_open_req_api, &error);
	}

	void send_close_req(int user_id, int dialogue_id,int version)
	{
		int error = 0;
		map_api_struct_t *send_api = NULL;
		map_close_request_t *close_req = NULL;

		send_api = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
		map_memzero(send_api,sizeof(map_api_struct_t));
		
		close_req = (map_close_request_t *)app_mem_get(sizeof(map_close_request_t));
		map_memzero(close_req,sizeof(map_close_request_t));

		fill_api_header(&(send_api->header),user_id);
		
		close_req->dialog_id = dialogue_id;
		close_req->is_corr_id = 0;
		close_req->rel_method = MAP_NORMAL_RELEASE;
		close_req->is_spec_info = 0;

		send_api->header.api_id = MAP_CLOSE_REQUEST;
		send_api->header.len = sizeof(map_close_request_t);
		send_api->header.ver = version;
		send_api->header.spare1 = 1;
		send_api->p_data = close_req;
		app_map_send_to_app_map((unsigned char *)send_api, &error);
			

	}

	void send_messages(char *api_name,int corr_id,int num_of_messages,int flag_dialogue)
	{
		
		unsigned char line_val[MAX_LINE_BYTE];
		char filename[100] = {'\0',},bitmap_array[20] = {'\0',};
		char *token_length = NULL,*token_val = NULL,*token_generic;
		FILE *fp_apis =NULL;
		int message_counter = 0,index = 0,counter = 0,buffer_val = 0,error = 0;
		map_api_struct_t  *send_api = NULL,*send_api_temp = NULL;

		memset(bitmap_array,'N',20);

		send_api = app_mem_get(sizeof(map_api_struct_t));
		map_memzero(send_api,sizeof(map_api_struct_t));


		sprintf(filename,"../buffers/%s",api_name);

		fp_apis = fopen(filename,"r");

		if(!strncmp(api_name,"MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST")))	
		{
				index = 0;

				map_mo_forward_sm_request_t	*p_v1_fsm_req = NULL,*p_v1_fsm_req_temp = NULL;

				printf("API Name Matches MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST \n");
				fill_api_header(&(send_api->header), msc_user_id);
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

				
				//if(1)
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


					


				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{
				
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v1_fsm_req_temp = (map_mo_forward_sm_request_t*)app_mem_get(sizeof(map_mo_forward_sm_request_t)); 
					map_memzero(p_v1_fsm_req_temp, sizeof(map_mo_forward_sm_request_t));

					

					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v1_fsm_req->header.corr_id =  corr_id;
							send_open_req(api_name ,corr_id);
						}
					}
					else{
					
						corr_id = app_map_get_new_correlation_id();
						p_v1_fsm_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}


					memcpy(p_v1_fsm_req_temp,p_v1_fsm_req,sizeof(map_mo_forward_sm_request_t));
					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));
					
					p_v1_fsm_req_temp->header.invoke_id = message_counter + 1;
					send_api_temp->p_data = p_v1_fsm_req_temp;
					
					if(message_counter == (num_of_messages -1))
						p_v1_fsm_req_temp->header.last_component = 1;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

				app_mem_free(p_v1_fsm_req);
				app_mem_free(send_api);
					

		}
		else if(!strncmp(api_name,"MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST")))
		{

				index = 0;  //Making index to 0 for next processing
				
				map_mt_forward_sm_request_t	*p_mt_fsm_req = NULL,*p_mt_fsm_req_temp = NULL;

				p_mt_fsm_req = (map_mt_forward_sm_request_t*)app_mem_get(sizeof(map_mt_forward_sm_request_t));
				map_memzero(p_mt_fsm_req,sizeof(map_mt_forward_sm_request_t));

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


				
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_mt_fsm_req_temp = (map_mt_forward_sm_request_t*)app_mem_get(sizeof(map_mt_forward_sm_request_t));
					map_memzero(p_mt_fsm_req_temp,sizeof(map_mt_forward_sm_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_mt_fsm_req->header.corr_id =  corr_id;
							send_open_req(api_name ,corr_id);
						}
					}
					else{
					
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_mt_fsm_req_temp,p_mt_fsm_req,sizeof(map_mt_forward_sm_request_t));
					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_mt_fsm_req_temp->header.invoke_id = message_counter + 1;
					send_api_temp->p_data = p_mt_fsm_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}


				app_mem_free(p_mt_fsm_req);
				app_mem_free(send_api);
					

		}
		else if(!strncmp(api_name,"MAP_V1_ALERT_SERVICE_CENTRE_REQUEST",strlen("MAP_V1_ALERT_SERVICE_CENTRE_REQUEST")))
		{

				index = 0;
				map_v1_alert_service_centre_without_result_request_t *p_alert_ser_centre_req = NULL,*p_alert_ser_centre_req_temp = NULL;

				p_alert_ser_centre_req = (map_v1_alert_service_centre_without_result_request_t *)\
							 app_mem_get (sizeof (map_v1_alert_service_centre_without_result_request_t));
				map_memzero(p_alert_ser_centre_req, sizeof (map_v1_alert_service_centre_without_result_request_t));

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


				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_alert_ser_centre_req_temp = (map_v1_alert_service_centre_without_result_request_t *)\
								 app_mem_get(sizeof (map_v1_alert_service_centre_without_result_request_t));
					map_memzero(p_alert_ser_centre_req_temp, sizeof (map_v1_alert_service_centre_without_result_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_alert_ser_centre_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
					
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_alert_ser_centre_req_temp,p_alert_ser_centre_req,sizeof (map_v1_alert_service_centre_without_result_request_t));
					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_alert_ser_centre_req_temp->header.invoke_id = message_counter + 1;
					send_api_temp->p_data = p_alert_ser_centre_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

				app_mem_free(send_api);
				app_mem_free(p_alert_ser_centre_req);
		}
		else{

			printf("Invalid API Hence continuing\n");
		}

		fclose(fp_apis);
		

	}



	void read_scenario()
	{
		FILE *fp = NULL;
		char *token_api_name=NULL,*token_num_message=NULL,*token_timer_duration =NULL,*token_generic = NULL,*token_dialog = NULL,*token_mul[10];
		unsigned char line_val[MAX_LINE_BYTE];
		int line_count = 0,counter = 0,token_counter =0,api_counter = 0;
		//map_api_struct_t                   *send_api = NULL;
		//map_mo_forward_sm_request_t	*p_v1_fsm_req = NULL;

		signal (SIGUSR1,sigusr1_hdlr);
		fflush(stdout); fflush(stdin);


		printf("Going to read app_map_user.conf file\n");



		//Freeing up the memory
		if(p_app_map_scenario != NULL)
			free(p_app_map_scenario);
		if(p_app_map_scenario_send != NULL)
			free(p_app_map_scenario_send);
		if(p_app_map_scenario_recv != NULL)
			free(p_app_map_scenario_recv);
			


		//Initialize the Global variables 
		number_of_dialogues = 0;
		
		//Open Configuration File
		fp = fopen("../conf/app_map_scene.conf","r");
		if(fp == NULL)
		{
			printf("Unable to open the app_map_user.conf file hence exiting \n");
			exit(1);
		}
		else{
			while(1)
			{
				if (!fgets(line_val, MAX_SCENE_LINE_BYTE, fp)) {

					printf("app_map_user.conf is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else if((*line_val == '[') && (*(line_val + 1) == 'S')) /*Start section */
				{

					line_count  = 0;
					if (!fgets(line_val, MAX_SCENE_LINE_BYTE, fp)) {
						printf("app_map_user.conf is successfully Parsed\n\n");
						break;
		
					}
					else{
						printf("Reading Number of scenarios\n");
						max_scenarios = atoi(line_val);
						max_scenarios_send = max_scenarios;
						//if(p_app_map_scenario != NULL)
						{
							//free(p_app_map_scenario);
						}
					//	else{

							p_app_map_scenario =(app_map_scenarios_t *) malloc(sizeof(app_map_scenarios_t) * max_scenarios);	
							p_app_map_scenario_send = p_app_map_scenario;
					//	}

					}
				}
				else if((*line_val == '[') && (*(line_val + 1) == 'R'))
				{
					line_count  = 0;
					if (!fgets(line_val, MAX_SCENE_LINE_BYTE, fp)) {
						printf("app_map_user.conf is successfully Parsed\n\n");
						break;
		
					}
					else{
						printf("Reading Number of scenarios\n");
						max_scenarios_recv = max_scenarios;
						max_scenarios = atoi(line_val);
						//if(p_app_map_scenario != NULL)
						{
							//free(p_app_map_scenario);
						}
						//else{

							p_app_map_scenario =(app_map_scenarios_t *) malloc(sizeof(app_map_scenarios_t) * max_scenarios);	
							p_app_map_scenario_recv = p_app_map_scenario;
					//	}

					}

				}
				else if((*line_val == '[') && (*(line_val + 1) == 'E'))
				{
					continue;
				}
				else{
						token_dialog = strtok(line_val," ");

						if(!strncmp(token_dialog,"SAME_DIALOGUE",strlen("SAME_DIALOGUE")))
						{
							token_api_name = strtok(NULL," ");
							token_num_message = strtok(NULL," ");
							token_timer_duration = strtok(NULL," ");

							p_app_map_scenario[line_count].p_app_map_apis = (app_map_apis_t *)malloc(sizeof(app_map_apis_t));

							p_app_map_scenario[line_count].is_same_dialogue = SAME_DIALOGUE;
							p_app_map_scenario[line_count].number_apis = 1;
							p_app_map_scenario[line_count].dialogue_id = INVALID_ID;

							p_app_map_scenario[line_count].p_app_map_apis[0].total_messages = atoi(token_num_message); 
							p_app_map_scenario[line_count].p_app_map_apis[0].timer_duration = atoi(token_timer_duration); 
							p_app_map_scenario[line_count].p_app_map_apis[0].count_messages = 0;
							
							strcpy(p_app_map_scenario[line_count].p_app_map_apis[0].api_name,token_api_name);

						}
						else if(!strncmp(token_dialog,"MULTIPLE_DIALOGUE",strlen("MULTIPLE_DIALOGUE")))
						{
							p_app_map_scenario[line_count].p_app_map_apis = (app_map_apis_t *)malloc(sizeof(app_map_apis_t));

							token_api_name = strtok(NULL," ");
							token_num_message = strtok(NULL," ");
							token_timer_duration = strtok(NULL," ");

							p_app_map_scenario[line_count].p_app_map_apis = (app_map_apis_t *)malloc(sizeof(app_map_apis_t));

							p_app_map_scenario[line_count].is_same_dialogue = MULTIPLE_DIALOGUE;
							p_app_map_scenario[line_count].number_apis = 1;

							p_app_map_scenario[line_count].p_app_map_apis[0].total_messages = atoi(token_num_message); 
							p_app_map_scenario[line_count].p_app_map_apis[0].timer_duration = atoi(token_timer_duration); 
							p_app_map_scenario[line_count].p_app_map_apis[0].count_messages = 0;

							strcpy(p_app_map_scenario[line_count].p_app_map_apis[0].api_name,token_api_name);

						}
						else if(!strncmp(token_dialog,"SAME_DIAL_MUL_MESSAGE",strlen("SAME_DIAL_MUL_MESSAGE")))
						{

							for(counter = 0;;counter++)
							{
								token_generic = strtok(NULL,":");
								if(token_generic != NULL)
								{
									token_mul[token_counter] = token_generic;
									token_counter++;
								}
								else{
									break;
								}
								
							}

							p_app_map_scenario[line_count].p_app_map_apis = (app_map_apis_t *)malloc(sizeof(app_map_apis_t) *token_counter);
							p_app_map_scenario[line_count].is_same_dialogue = SAME_DIAL_MUL_MESSAGE;
							p_app_map_scenario[line_count].number_apis = token_counter;


							for(counter = 0;counter < token_counter ; counter++)
							{

								token_api_name = strtok(token_mul[counter]," ");
								token_num_message = strtok(NULL," ");
								token_timer_duration = strtok(NULL," ");


								p_app_map_scenario[line_count].p_app_map_apis[counter].total_messages = atoi(token_num_message); 
								p_app_map_scenario[line_count].p_app_map_apis[counter].timer_duration = atoi(token_timer_duration); 
								p_app_map_scenario[line_count].p_app_map_apis[counter].count_messages = 0;

								strcpy(p_app_map_scenario[line_count].p_app_map_apis[counter].api_name,token_api_name);


							}
							

						}
						else{
							printf("Illegal Option for Dialogue Hence exiting\n");
							exit(1);
						}

						line_count++;
					
					}

				}
		}

		fclose(fp);

		//Process the SCENARIOS Let us assume File name is same as API Name

		for(counter = 0;counter <line_count ;counter++ )
		{


			for(api_counter = 0; api_counter < p_app_map_scenario_send[counter].number_apis;api_counter++)
			{

				//send_open_req(corr_id);

				//for(total_mess_counter = 0;total_mess_counter < p_app_map_scenario[line_count].p_app_map_apis[api_counter].total_messages;total_mess_counter++)
				{
					//send_messages(p_app_map_scenario_send[counter].p_app_map_apis[api_counter].api_name,corr_id,p_app_map_scenario_send[counter].p_app_map_apis[api_counter].total_messages,p_app_map_scenario_send[counter].is_same_dialogue);
				}


			}

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
			int flag =0;
			signal (SIGUSR1, sigusr1_hdlr);
			signal(SIGUSR2, print_values);

			while (-1 != flag)
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


			return (void *)1;
	}

	void print_values(int sig)
	{
		signal(SIGUSR2, print_values);
		fflush(stdout);

		printf(" Open requests sent :: %d\n Close Indication received :: %d\n MTFSM Requests sent :: %d\n MTFSM Confirms received :: %d\n\n", (int)open_req_c,(int)close_ind_c,(int)mtfsm_v3_req_c,(int) mtfsm_v3_cnf_c);
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

	signal (SIGUSR2, print_values);
	signal (SIGUSR1, sigusr1_hdlr);

	i = 1;	

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


/* Function to fill the parameters of MAP OPEN Response */
void fill_open_resp(map_open_response_t *open,map_open_indication_t *open_ind)
{
	open->dialog_id = open_ind->dialog_id;
	open->result = MAP_DIALOGUE_ACCEPTED;
	open->is_acn = MAP_TRUE;
	open->app_cont_name.appl_context = open_ind->app_cont_name.appl_context ;
	open->app_cont_name.version =  open_ind->app_cont_name.version;
	open->is_resp_add = MAP_TRUE;
	open->resp_add.routing_ind = ROUTE_ON_SSN;
	open->resp_add.is_spc = 1;
	open->resp_add.spc = g_spc ;
	open->resp_add.is_ssn = MAP_TRUE;
	open->resp_add.ssn = origSSN; /* GSM SCF */
	open->resp_add.is_gt = 0;
	open->resp_add.global_title_ind = MAP_GT_WITH_TT_NP_ES_NAI; /*Section 3.4.1 possible values = MAP_NO_GT,MAP_GT_WIT
								      H_NAI,MAP_GT_WITH_TT,MAP_GT_WITH_TT_NP_ES,MAP_GT_WITH_TT_NP_ES_NAI,MAP_GT_RESERVED*/
	open->resp_add.gt.type4.translation_type = 2;
	open->resp_add.gt.type4.numbering_plan = 2;
	open->resp_add.gt.type4.encoding_scheme = 2;
	open->resp_add.gt.type4.nature_of_addr_ind = 2;
	open->resp_add.gt.type4.num_gt_addr_info_octets = 5;
	open->resp_add.gt.type4.gt_addr_info[0] = 0x22;
	open->resp_add.gt.type4.gt_addr_info[1] = 0x22;
	open->resp_add.gt.type4.gt_addr_info[2] = 0x22;
	open->resp_add.gt.type4.gt_addr_info[3] = 0x22;
	open->resp_add.gt.type4.gt_addr_info[4] = 0x22;
	open->is_qos = MAP_FALSE;
	open->is_spec_info = open_ind->is_spec_info;
	open->is_ref_reason = MAP_FALSE;
}


void map_fill_mtfsm_v3_res_hdr(map_service_header_t *mt_forw_sm_resp_hdr, map_service_header_t mt_forw_sm_ind_hdr)
{
	mt_forw_sm_resp_hdr->dlg_id = mt_forw_sm_ind_hdr.dlg_id;
	mt_forw_sm_resp_hdr->is_corr_id = MAP_FALSE;
	mt_forw_sm_resp_hdr->invoke_id = mt_forw_sm_ind_hdr.invoke_id;
	mt_forw_sm_resp_hdr->is_linked_id = MAP_FALSE;
	mt_forw_sm_resp_hdr->last_component = MAP_TRUE ;
}
void map_fill_mtfsm_v3_res(map_mt_forward_sm_response_t *mt_forw_sm_res)
{ 
	mt_forw_sm_res->choice= MAP_RESULT;
	mt_forw_sm_res->response.result.is_sm_rp_ui = MAP_FALSE;
	mt_forw_sm_res->response.result.is_extension = MAP_FALSE;
}


#if 0
void map_fill_mofsm_v3_res(map_mo_forward_sm_response_t *mo_forw_sm_res)
{ 
	//static int i =0;

	//mo_forw_sm_res->choice= MAP_USER_ERROR ;
	mo_forw_sm_res->choice= MAP_NO_INFO ;

	//switch ((i++)%3)
	switch (5)
	{	
		case 0 :
			mo_forw_sm_res->response.user_error.error_code = 32 ; //MAP_SM_DELIVERY_FAILURE
			break;	
		case 1 :
			mo_forw_sm_res->response.user_error.error_code = 31 ; //MAP_SUBSCRIBER_BUSY_FOR_MT_SMS
			break;	
		case 2 :
			mo_forw_sm_res->response.user_error.error_code = 6 ; //MAP_ABSENT_SUBSCRIBER_SM 
			break;	
	}

	//mo_forw_sm_res->response.user_error.is_parameter_present = MAP_TRUE;
	mo_forw_sm_res->response.user_error.is_parameter_present = MAP_FALSE;

	mo_forw_sm_res->response.user_error.user_error.sm_del_fail_cause.sm_enumerated_delivery_failure_cause = 0; // MAP_SM_ENUMERATED_DELIVERY_FAILURE_CAUSE_MEMORY_CAPACITY_EXCEEDED
	mo_forw_sm_res->response.user_error.user_error.sm_del_fail_cause.is_diagnostic_info = MAP_FALSE;
	mo_forw_sm_res->response.user_error.user_error.sm_del_fail_cause.is_extension = MAP_FALSE;

	/*mo_forw_sm_res->response.result.is_sm_rp_ui = MAP_FALSE;
	  mo_forw_sm_res->response.result.sm_rp_ui.length = 5;
	  mo_forw_sm_res->response.result.sm_rp_ui.value[0] =0xAA;
	  mo_forw_sm_res->response.result.sm_rp_ui.value[1] =0xBB;
	  mo_forw_sm_res->response.result.sm_rp_ui.value[2] =0xCC;
	  mo_forw_sm_res->response.result.sm_rp_ui.value[3] =0xDD;
	  mo_forw_sm_res->response.result.sm_rp_ui.value[4] =0xEE;

	  mo_forw_sm_res->response.result.is_extension = MAP_FALSE;*/
}

#endif

void fill_user_error(map_user_error_t *p_user_error,FILE *p_file,char *bitmap)
{
	
	unsigned char line_val[MAX_LINE_BYTE];
	int index = 0,counter =0;
	char *token_length = NULL,*token_val = NULL,*token_generic =NULL;
	

	while(1)
	{
		if (!fgets(line_val, MAX_LINE_BYTE, p_file)) {

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
			if(bitmap[index] == '1')
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
					}		

				}	

			}

		}

	}
}

void map_fill_mofsm_v3_res(map_mo_forward_sm_response_t *mo_forw_sm_res)
{	
	unsigned char line_val[MAX_LINE_BYTE];
	FILE *fp_apis = NULL;
	char filename[100] = {'\0',},bitmap_array[20] = {'\0',};
	char * token_length = NULL,*token_generic = NULL,*token_val = NULL,*token_ext = NULL;
	int ext_counter = 0,counter = 0,element_number = 0,index = 0;
	sprintf(filename,"../buffers/%s","MAP_V3_FORWARD_SHORT_MESSAGE_RESPONSE");
	extension_container *p_container = NULL;
	fp_apis = fopen(filename,"r");
	

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
						mo_forw_sm_res->choice= atoi(token_generic);
					}		
					else if(index == 1)
						mo_forw_sm_res->response.result.is_sm_rp_ui = atoi(token_generic);
					else if(index == 2)
					{
						mo_forw_sm_res->response.result.sm_rp_ui.length =  atoi(token_length);
						mo_forw_sm_res->response.result.sm_rp_ui.value[counter] =  hstoi(token_generic);
					}
					else if(index == 3)
					{

						mo_forw_sm_res->response.result.is_extension = atoi(token_generic);
					}
					else if(index == 4)
					{
						mo_forw_sm_res->response.result.extension_container.map_pvt_ext_count = atoi(token_length);

						for(ext_counter = 0; ext_counter <mo_forw_sm_res->response.result.extension_container.map_pvt_ext_count;ext_counter++ )
						{
							int finished_flag = 0;
							if(ext_counter ==0)	
							{
								token_ext = strtok(token_val,":");
							}		
							else{
								token_ext = strtok(NULL,":");
							}

							while(1)
							{

								if(finished_flag == 0)
									token_generic = strtok(token_ext,",");
								else
									token_generic = strtok(NULL,",");

								if(token_generic == NULL)
									break;
								else	
								{
									if(finished_flag == 0)
									{
										mo_forw_sm_res->response.result.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


									}
									else
									{
										mo_forw_sm_res->response.result.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
									}
								}

								finished_flag++;

							}


						}

					}
					else if(index == 5)
					{

						for(ext_counter = 0; ext_counter <mo_forw_sm_res->response.result.extension_container.map_pvt_ext_count;ext_counter++ )
						{
							int finished_flag = 0;
							if(ext_counter ==0)	
							{
								token_ext = strtok(token_val,":");
							}		
							else{
								token_ext = strtok(NULL,":");
							}


							if(!strncmp(token_ext,"NULL",strlen("NULL")))
								mo_forw_sm_res->response.result.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
							else
							{	
								mo_forw_sm_res->response.result.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
								while(1)
								{

									if(finished_flag == 0)
										token_generic = strtok(token_ext,",");
									else
										token_generic = strtok(NULL,",");

									if(token_generic == NULL)
										break;
									else	
									{
										if(finished_flag == 0)
										{
											mo_forw_sm_res->response.result.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


										}
										else
										{
											mo_forw_sm_res->response.result.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
										}
									}

									finished_flag++;

								}
							}

						}
					}
					else if(index == 6)
						mo_forw_sm_res->response.user_error.error_code = hstoi(token_generic);
					else if(index == 7)
						mo_forw_sm_res->response.user_error.is_parameter_present = atoi(token_generic);
					else if(index == 8)
						element_number  = atoi(token_generic);
					else{

						switch(element_number)
						{
					   	   case 1:
						   {
							if(index == 9)	
						  	  mo_forw_sm_res->response.user_error.user_error.sys_failure.choice = hstoi(token_generic);
							else if(index == 10)
							{
							    if(mo_forw_sm_res->response.user_error.user_error.sys_failure.choice == 1 )
							       mo_forw_sm_res->response.user_error.user_error.sys_failure.u.network_resource = atoi(token_generic);

							}				
							else if(index == 11)
							{

							    if(mo_forw_sm_res->response.user_error.user_error.sys_failure.choice == 2 )
							     mo_forw_sm_res->response.user_error.user_error.sys_failure.u.extensible_system_failure_param.is_network_resource = atoi(token_generic);
							}
							else if(index == 12)
							{

							      if(mo_forw_sm_res->response.user_error.user_error.sys_failure.choice == 2 )
									mo_forw_sm_res->response.user_error.user_error.sys_failure.u.extensible_system_failure_param.network_resource = atoi(token_generic);
							}
							else if(index == 13)
							{
								if(mo_forw_sm_res->response.user_error.user_error.sys_failure.choice == 2 )
								mo_forw_sm_res->response.user_error.user_error.sys_failure.u.extensible_system_failure_param.is_extension = atoi(token_generic);
							}
							else if(index == 14)
							{
								mo_forw_sm_res->response.user_error.user_error.sys_failure.u.extensible_system_failure_param.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter <mo_forw_sm_res->response.user_error.user_error.sys_failure.u.extensible_system_failure_param.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val,":");
									}		
									else{
										token_ext = strtok(NULL,":");
									}

									while(1)
									{

									if(finished_flag == 0)
										token_generic = strtok(token_ext,",");
									else
										token_generic = strtok(NULL,",");

										if(token_generic == NULL)
											break;
										else	
										{
											if(finished_flag == 0)
											{
												mo_forw_sm_res->response.user_error.user_error.sys_failure.u.extensible_system_failure_param.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												mo_forw_sm_res->response.user_error.user_error.sys_failure.u.extensible_system_failure_param.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}


								}

							}
							else if(index == 15)
							{

								for(ext_counter = 0; ext_counter <mo_forw_sm_res->response.user_error.user_error.sys_failure.u.extensible_system_failure_param.extension_container.map_pvt_ext_count;ext_counter++ )
								{
											int finished_flag = 0;
											if(ext_counter ==0)	
											{
												token_ext = strtok(token_val,":");
											}		
											else{
												token_ext = strtok(NULL,":");
											}


											if(!strncmp(token_ext,"NULL",strlen("NULL")))
												mo_forw_sm_res->response.user_error.user_error.sys_failure.u.extensible_system_failure_param.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
											else
											{	
												mo_forw_sm_res->response.user_error.user_error.sys_failure.u.extensible_system_failure_param.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
												while(1)
												{

													if(finished_flag == 0)
														token_generic = strtok(token_ext,",");
													else
														token_generic = strtok(NULL,",");

													if(token_generic == NULL)
														break;
													else	
													{
														if(finished_flag == 0)
														{
															mo_forw_sm_res->response.user_error.user_error.sys_failure.u.extensible_system_failure_param.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


														}
														else
														{
															mo_forw_sm_res->response.user_error.user_error.sys_failure.u.extensible_system_failure_param.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
														}
													}

													finished_flag++;

												}
											}

										}
								}
							}
						break;

						case 2:
						{
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.data_missing.extension_container);	

							if(index == 16)	
							{
								mo_forw_sm_res->response.user_error.user_error.data_missing.is_extension = atoi(token_generic);		
								break;
							}
						}		
						case 3:
						{
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.unexpec_data.extension_container);	
							if(index == 16)	
							{
								mo_forw_sm_res->response.user_error.user_error.unexpec_data.is_extension = atoi(token_generic);				
								break;
							}
						}
						case 4:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.incompat_term.extension_container);	
							if(index == 16)	
							{
								mo_forw_sm_res->response.user_error.user_error.incompat_term.is_extension = atoi(token_generic);		
								break;
							}
						case 5:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.res_limitation.extension_container);	
							if(index == 16)	
							{
								mo_forw_sm_res->response.user_error.user_error.res_limitation.is_extension = atoi(token_generic);				      break;	
							}
						case 6:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.num_changed.extension_container);	
							if(index == 16)
							{	
								mo_forw_sm_res->response.user_error.user_error.res_limitation.is_extension = atoi(token_generic);		
								break;
							}
						case 7:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.unidentified_sub.extension_container);	
							if(index == 16)	
							{
								mo_forw_sm_res->response.user_error.user_error.res_limitation.is_extension = atoi(token_generic);				      break;
							}
						case 8:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.illegal_sub.extension_container);	
							if(index == 16)	
							{
								mo_forw_sm_res->response.user_error.user_error.illegal_sub.is_extension = atoi(token_generic);					      break;	
							}
						case 9:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.illegal_equip.extension_container);	
							if(index == 16)	
							{
								mo_forw_sm_res->response.user_error.user_error.illegal_equip.is_extension = atoi(token_generic);				      break;	
							}
						case 10:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.bearer_serv_not_prov.extension_container);	
							if(index == 16)	
							{
								mo_forw_sm_res->response.user_error.user_error.bearer_serv_not_prov.is_extension = atoi(token_generic);				      break;
							}
						case 11:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.teleserv_not_prov.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.teleserv_not_prov.is_extension = atoi(token_generic);		
						case 12:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.target_cell_out_gca.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.target_cell_out_gca.is_extension = atoi(token_generic);		
						case 13:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.tracing_buffer_full.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.tracing_buffer_full.is_extension = atoi(token_generic);		
						case 14:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.no_roaming_nb.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.no_roaming_nb.is_extension = atoi(token_generic);		
						case 15:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.no_sub_reply.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.no_sub_reply.is_extension = atoi(token_generic);		
						case 16:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.forwarding_violation.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.forwarding_violation.is_extension = atoi(token_generic);		
						case 17:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.forwarding_failed.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.forwarding_failed.is_extension = atoi(token_generic);		
						case 18:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.or_not_allowed.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.or_not_allowed.is_extension = atoi(token_generic);		
						case 19:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.ati_not_allowed.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.ati_not_allowed.is_extension = atoi(token_generic);		
						case 20:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.atsi_not_allowed.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.atsi_not_allowed.is_extension = atoi(token_generic);		
						case 21:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.atm_not_allowed.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.atm_not_allowed.is_extension = atoi(token_generic);		
						case 22:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.info_not_avail.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.info_not_avail.is_extension = atoi(token_generic);		
						case 23:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.illegal_ss_op.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.illegal_ss_op.is_extension = atoi(token_generic);		
						case 24:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.ss_not_avail.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.ss_not_avail.is_extension = atoi(token_generic);		
						case 25:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.ss_sub_violation.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.ss_sub_violation.is_extension = atoi(token_generic);		
						case 26:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.mesg_wait_list_full.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.mesg_wait_list_full.is_extension = atoi(token_generic);		
						case 27:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.no_group_call_nb.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.no_group_call_nb.is_extension = atoi(token_generic);		
						case 28:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.unauth_req_network.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.unauth_req_network.is_extension = atoi(token_generic);		
						case 29:
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.unk_or_unreach_lcscli.extension_container);	
							if(index == 16)	
							mo_forw_sm_res->response.user_error.user_error.unk_or_unreach_lcscli.is_extension = atoi(token_generic);		
																				         case 30:
			 			{
							if(NULL == p_container)
							 p_container =(extension_container *) &(mo_forw_sm_res->response.user_error.user_error.mm_event_not_supp.extension_container);	
							if(index == 16)	
								mo_forw_sm_res->response.user_error.user_error.mm_event_not_supp.is_extension = atoi(token_generic);		
							else if(index == 17)
							{
								p_container->map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter <p_container->map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val,":");
									}		
									else{
										token_ext = strtok(NULL,":");
									}

									while(1)
									{

										if(finished_flag == 0)
											token_generic = strtok(token_ext,",");
										else
											token_generic = strtok(NULL,",");

										if(token_generic == NULL)
											break;
										else	
										{
											if(finished_flag == 0)
											{
												p_container->private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_container->private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}


								}

							}
							else if(index == 18)
							{

								for(ext_counter = 0; ext_counter <p_container->map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val,":");
									}		
									else{
										token_ext = strtok(NULL,":");
									}


									if(!strncmp(token_ext,"NULL",strlen("NULL")))
										p_container->private_ext_list[ext_counter].is_ext_type = 0;
									else
									{	
										p_container->private_ext_list[ext_counter].is_ext_type = 1;
										while(1)
										{

											if(finished_flag == 0)
												token_generic = strtok(token_ext,",");
											else
												token_generic = strtok(NULL,",");

											if(token_generic == NULL)
												break;
											else	
											{
												if(finished_flag == 0)
												{
													p_container->private_ext_list[ext_counter].enc_len= atoi(token_generic);


												}
												else
												{
													p_container->private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
												}
											}

											finished_flag++;

										}
									}

								}
							}

						}

					    }

					  }

					index++;
					
			  	}

		   	}
		}

	   }

	
}
void map_fill_mofsm_v3_res_hdr(map_service_header_t *mo_forw_sm_resp_hdr, map_service_header_t mo_forw_sm_ind_hdr)
{
	mo_forw_sm_resp_hdr->dlg_id = mo_forw_sm_ind_hdr.dlg_id;
	mo_forw_sm_resp_hdr->is_corr_id = MAP_FALSE;
	mo_forw_sm_resp_hdr->invoke_id = mo_forw_sm_ind_hdr.invoke_id;
	mo_forw_sm_resp_hdr->is_linked_id = MAP_FALSE;
	mo_forw_sm_resp_hdr->last_component = MAP_TRUE;
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
	header->last_component = 0;
}



void send_mt_forw_resp(int user_id,int version,map_service_header_t mt_forw_sm_ind_hdr,int last_flag)
{
	int error =0;
	map_mt_forward_sm_response_t *mt_fsm_res = NULL;
	map_api_struct_t *send_api = NULL;

	send_api = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
	map_memzero(send_api,sizeof(map_api_struct_t));
	mt_fsm_res = (map_mt_forward_sm_response_t *)app_mem_get(sizeof(map_mt_forward_sm_response_t));
	map_memzero(mt_fsm_res,sizeof(map_mt_forward_sm_response_t));


	fill_api_header(&(send_api->header),user_id);
	map_fill_mtfsm_v3_res_hdr(&(mt_fsm_res->header),mt_forw_sm_ind_hdr);

	mt_fsm_res->header.last_component = last_flag;

	map_fill_mtfsm_v3_res(mt_fsm_res);
	send_api->header.api_id = MAP_MT_FORWARD_SHORT_MESSAGE_RESPONSE;
	send_api->header.len = sizeof(map_mt_forward_sm_response_t);
	send_api->header.ver = version;
	send_api->header.spare1 = 1;
	send_api->p_data = mt_fsm_res;
	app_map_send_to_app_map((unsigned char *)send_api, &error);
}


void send_mo_forw_resp(int user_id,int version,map_service_header_t mo_forw_sm_ind_hdr,int last_flag)
{
	int error =0;
	map_mo_forward_sm_response_t *mo_fsm_res = NULL;
	map_api_struct_t *send_api = NULL;

	send_api = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
	map_memzero(send_api,sizeof(map_api_struct_t));
	mo_fsm_res = (map_mo_forward_sm_response_t *)app_mem_get(sizeof(map_mo_forward_sm_response_t));
	map_memzero(mo_fsm_res,sizeof(map_mo_forward_sm_response_t));


	fill_api_header(&(send_api->header),user_id);
	map_fill_mofsm_v3_res_hdr(&(mo_fsm_res->header),mo_forw_sm_ind_hdr);

	mo_fsm_res->header.last_component = last_flag;

	map_fill_mofsm_v3_res(mo_fsm_res);

	if(1 == mo_fsm_res->response.user_error.is_parameter_present)
	{
		//Hardcode the Value here for Error
		mo_fsm_res->response.user_error.user_error.sys_failure.choice = 1;
		mo_fsm_res->response.user_error.user_error.sys_failure.u.network_resource = 4;	

	}
	send_api->header.api_id = MAP_MO_FORWARD_SHORT_MESSAGE_RESPONSE;
	send_api->header.len = sizeof(map_mo_forward_sm_response_t);
	send_api->header.ver = version;
	send_api->header.spare1 = 1;
	send_api->p_data = mo_fsm_res;
	app_map_send_to_app_map((unsigned char *)send_api, &error);
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
	map_api_struct_t *send_api = NULL;
	int error = 0,counter = 0;

	p_api = (map_api_struct_t *)p_buffer;
	send_api = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
	map_memzero(send_api,sizeof(map_api_struct_t));

	fill_api_header(&(send_api->header),msc_user_id);


	signal(SIGUSR1, sigusr1_hdlr);



	switch(p_api->header.api_id)
	{
		case  MAP_APP_SAP_SSN_STATE_INDICATION:
			{
				map_app_sap_ssn_state_indication_t  *open_ind;
				open_ind = (map_app_sap_ssn_state_indication_t *)p_api->p_data;
				printf("Received SAP STATE IND state [%d] SSN [%d] SAP ID [%d]\n", open_ind->state, open_ind->ssn, open_ind->sap_id);

			}
			break;

		case MAP_REGISTER_USER_CONFIRM:
			{
				printf("Received MAP_REGISTER_USER_CONFIRM \n");
				user_registered = 1;
			}
			break;

		case MAP_DEREGISTER_USER_CONFIRM:
			{
				map_deregister_user_confirm_t *dereg_conf;

				printf("\nReceived MAP_DEREGISTER_USER_CONFIRM from MAP Stack\n");
				if( dereg_conf->result != MAP_SUCCESS )
				{
					free(p_api->p_data);
					printf("\nDeregister FAILED\n");
					/* exit(1); */
				}
				else
				{
					free(p_api->p_data);
					printf("\nDeregister success\n");
					exit(0);
				}
			}

			break;

		case MAP_OPEN_INDICATION :/* Received the MAP_OPEN_INDICATION  */

			{
				map_open_response_t *open_resp = NULL;
				map_open_indication_t *open_ind = NULL;
				if (give_trace) printf("Received MAP_OPEN_INDICATION \n");
				open_ind_c++;	
				/*Prepare the MAP OPEN Response */
				open_ind = (map_open_indication_t *)p_api->p_data;
				open_resp = (map_open_response_t *)app_mem_get(sizeof(map_open_response_t));
				map_memzero(open_resp,sizeof(map_open_response_t));

				fill_open_resp(open_resp,open_ind);

				open_resp->resp_add.is_ssn = 1;
				open_resp->resp_add.ssn = origSSN; /* GSM SCF */

				open_resp->resp_add.routing_ind = ROUTE_ON_SSN;
				open_resp->resp_add.is_spc=1;
				open_resp->resp_add.is_gt=0;
				send_api->header.api_id = MAP_OPEN_RESPONSE;
				send_api->header.len = sizeof(map_open_response_t);
				send_api->header.ver =  3 ;
				/* Fill the SAP in spare 1*/
				send_api->header.spare1 = 1;

				send_api->p_data = open_resp;

				/*send the open response */
				if(app_map_send_to_app_map((unsigned char *)send_api, &error) == APP_FAILURE)
				{
					printf("ERROR IN SENDING.....ECODE=%d\n",error);
				}


			}
			break;

		case MAP_DELIMITER_INDICATION:
			{
				map_delimiter_indication_t *delim_ind = NULL;
				app_mem_free(send_api);
				delim_ind = (map_delimiter_indication_t *)p_api->p_data;
			}
			break;


		case MAP_MT_FORWARD_SHORT_MESSAGE_INDICATION:
			{
				map_user_error_t p_temp;
				p_temp.error_code = 41;
				

				map_mt_forward_sm_indication_t *mt_fsm_ind = NULL;
				mt_fsm_ind = (map_mt_forward_sm_indication_t *)p_api->p_data;

				p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].count_messages++;
				printf("\nReceived MAP_MT_FORWARD_SHORT_MESSAGE_INDICATION  with Total Mess [%d] and Count [%d]- ",p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].total_messages ,p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].count_messages); 

				if(SAME_DIALOGUE == p_app_map_scenario_recv[index_scenarios].is_same_dialogue )
				{
					if(!strncmp(p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].api_name,"MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST")) && ((p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].total_messages )> (p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].count_messages )))
					{
						//p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages++;
						send_mt_forw_resp(p_api->header.user_id,p_api->header.ver,mt_fsm_ind->header,0);
						printf("Sending MO RESP Request #################");
						//send_response but not close the dialogue

					}
					else if(!strncmp(p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].api_name,"MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST")) && (p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].total_messages ) == (p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].count_messages))

					{
						//send close request and response
						send_mt_forw_resp(p_api->header.user_id,p_api->header.ver,mt_fsm_ind->header,1);
						send_close_req(p_api->header.user_id,mt_fsm_ind->header.dlg_id,p_api->header.ver);
						printf("Sending Close Request #################\n");

						index_scenarios++;
						if(index_scenarios == max_scenarios_recv)
						{
							//Init the Count and index scenarios
							index_scenarios = 0;
							for(counter = 0; counter < max_scenarios_recv ; counter++)
							{
								p_app_map_scenario_recv[counter].p_app_map_apis[0].count_messages = 0;
							}
						}
					}
					else{

						printf("Unknown Dialogue Id is Recived hence continuing \n");
					}

				}
				else if(MULTIPLE_DIALOGUE == p_app_map_scenario_recv[index_scenarios].is_same_dialogue)
				{

					if(p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].total_messages == (p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].count_messages))
					{
						index_scenarios++;
						if(index_scenarios == max_scenarios_recv)
						{
							//Init the Count and index scenarios
							index_scenarios = 0;
							//p_app_map_scenario_recv[index_scenarios].p_app_map_apis[1].count_messages = 0;
							for(counter = 0; counter < max_scenarios_recv ; counter++)
							{
								p_app_map_scenario_recv[counter].p_app_map_apis[0].count_messages = 0;
							}
						}

					}

					//p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages++;
					send_mt_forw_resp(p_api->header.user_id,p_api->header.ver,mt_fsm_ind->header,1);
					send_close_req(p_api->header.user_id,mt_fsm_ind->header.dlg_id,p_api->header.ver);



				}


			}
			break;

		case MAP_MO_FORWARD_SHORT_MESSAGE_INDICATION:
		{
				map_mo_forward_sm_indication_t *mo_fsm_ind = NULL;
				mo_fsm_ind = (map_mo_forward_sm_indication_t *)p_api->p_data;

				//for(api_counter = 0; api_counter < p_app_map_scenario_recv[index_scenarios].number_apis; api_counter++)
				{
					p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].count_messages++;
					printf("\nReceived MAP_MO_FORWARD_SHORT_MESSAGE_INDICATION  with Total Mess [%d] and Count [%d]- ",p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].total_messages ,p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].count_messages); 

					if(SAME_DIALOGUE == p_app_map_scenario_recv[index_scenarios].is_same_dialogue )
					{
						if(!strncmp(p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].api_name,"MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST")) && ((p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].total_messages )> (p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].count_messages )))
						{
							//p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages++;
							send_mo_forw_resp(p_api->header.user_id,p_api->header.ver,mo_fsm_ind->header,0);
							printf("Sending MO RESP Request #################");
							//send_response but not close the dialogue

						}
						else if(!strncmp(p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].api_name,"MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST")) && (p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].total_messages ) == (p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].count_messages))

						{
							//send close request and response
							send_mo_forw_resp(p_api->header.user_id,p_api->header.ver,mo_fsm_ind->header,1);
							send_close_req(p_api->header.user_id,mo_fsm_ind->header.dlg_id,p_api->header.ver);
							printf("Sending Close Request #################\n");

							index_scenarios++;
							if(index_scenarios == max_scenarios_recv)
							{
								//Init the Count and index scenarios
								index_scenarios = 0;
								for(counter = 0; counter < max_scenarios_recv ; counter++)
								{
									p_app_map_scenario_recv[counter].p_app_map_apis[0].count_messages = 0;
								}
							}
						}
						else{

							printf("Unknown Dialogue Id is Recived hence continuing \n");
						}

					}
					else if(MULTIPLE_DIALOGUE == p_app_map_scenario_recv[index_scenarios].is_same_dialogue)
					{

						if(p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].total_messages == (p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].count_messages))
						{
							index_scenarios++;
							if(index_scenarios == max_scenarios_recv)
							{
								//Init the Count and index scenarios
								index_scenarios = 0;
								//p_app_map_scenario_recv[index_scenarios].p_app_map_apis[1].count_messages = 0;
								for(counter = 0; counter < max_scenarios_recv ; counter++)
								{
									p_app_map_scenario_recv[counter].p_app_map_apis[0].count_messages = 0;
								}
							}

						}

						//p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages++;
						send_mo_forw_resp(p_api->header.user_id,p_api->header.ver,mo_fsm_ind->header,1);
						send_close_req(p_api->header.user_id,mo_fsm_ind->header.dlg_id,p_api->header.ver);



					}

				}



#if 0
				for(counter = 0;counter < max_scenarios_recv;counter++)
				{
					if(p_app_map_scenario_recv[counter].dialogue_id == mo_fsm_ind->header.dlg_id)
					{
						is_found = 1;
						break;
					}
					else if(p_app_map_scenario_recv[counter].dialogue_id == INVALID_ID)
					{
						is_found = 0;
						break
					}

				}

				if(!is_found)
				{

					//New Dialogue
					p_app_map_scenario_recv[counter].dialogue_id = mo_fsm_ind->header.dlg_id;
					//number_of_dialogues++;
				}



				for(counter = 0;counter < max_scenarios_recv;counter++)
				{
					if(p_app_map_scenario_recv[counter].dialogue_id == mo_fsm_ind->header.dlg_id)
					{
						for(api_counter = 0; api_counter < p_app_map_scenario_recv[counter].number_apis; api_counter++)
						{
							if(!strncmp(p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].api_name,"MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST")) && (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].total_messages > (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages - 1)))
							{
								p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages++;
								send_mo_forw_resp(p_api->header.user_id,p_api->header.ver,mo_fsm_ind->header,1);
								//send_response but not close the dialogue

							}
							else if(!strncmp(p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].api_name,"MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST")) && (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].total_messages == (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages - 1)))

							{
								p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages++;
								//send close request and response
								send_mo_forw_resp(p_api->header.user_id,p_api->header.ver,mo_fsm_ind->header,0);
								send_close_req(p_api->header.user_id,mo_fsm_ind->header.dlg_id,p_api->header.ver);
							}
							else{
								printf("Unknown Dialogue Id is Recived hence continuing \n");
							}


						}

					}
				}


#endif 

		}
			break;

		case  MAP_ALERT_SERVICE_CENTRE_WITHOUT_RESULT_INDICATION:
			{
				map_v1_alert_service_centre_without_result_indication_t *p_alert_ind = NULL;
				p_alert_ind = (map_v1_alert_service_centre_without_result_indication_t *)p_api->p_data;

				printf("MAP_V1_ALERT_SERVICE_CENTRE_WITHOUT_RESULT_INDICATION Recieved \n");
				p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].count_messages++;

				if(SAME_DIALOGUE == p_app_map_scenario_recv[index_scenarios].is_same_dialogue )
				{	
					if((p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].total_messages ) == (p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].count_messages))
					{
						//Send Close request
						index_scenarios++;
						send_close_req(p_api->header.user_id,p_alert_ind->header.dlg_id,p_api->header.ver);
						if(index_scenarios == max_scenarios_recv)
						{
							//Init the Count and index scenarios
							index_scenarios = 0;
							for(counter = 0; counter < max_scenarios_recv ; counter++)
							{
								p_app_map_scenario_recv[counter].p_app_map_apis[0].count_messages = 0;
							}
						}

					}

				}
				else if(MULTIPLE_DIALOGUE == p_app_map_scenario_recv[index_scenarios].is_same_dialogue )
				{
					send_close_req(p_api->header.user_id,p_alert_ind->header.dlg_id,p_api->header.ver);

					if((p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].total_messages ) == (p_app_map_scenario_recv[index_scenarios].p_app_map_apis[0].count_messages))
					{
						index_scenarios++;

						if(index_scenarios == max_scenarios_recv)
						{
							//Init the Count and index scenarios
							index_scenarios = 0;
							for(counter = 0; counter < max_scenarios_recv ; counter++)
							{
								p_app_map_scenario_recv[counter].p_app_map_apis[0].count_messages = 0;
							}
						}

					}
					//Send close
				}

			}
			break;
		case MAP_ALERT_SERVICE_CENTRE_INDICATION:
		{
			printf("MAP_ALERT_SERVICE_CENTRE_INDICATION Indication recieved\n");
		}
		break;
		case MAP_INFORM_SERVICE_CENTRE_INDICATION:
		{
			printf("MAP_INFORM_SERVICE_CENTRE_INDICATION Indication recieved\n");
		}
		break;
		case MAP_REPORT_SM_DELIVERY_STATUS_INDICATION:
		{
			printf("MAP_REPORT_SM_DELIVERY_STATUS_INDICATION Indication recieved\n");
		}
		break;


		case MAP_MO_FORWARD_SHORT_MESSAGE_CONFIRM:
			{

				map_mo_forward_sm_confirm_t *mo_fsm_conf = (map_mo_forward_sm_confirm_t *)p_api->p_data;
				printf("Received  MAP_MO_FORWARD_SHORT_MESSAGE_CONFIRM from MAP stack\n");

				if (give_trace)printf("Choice [%u]\n", mo_fsm_conf->choice );

				if ( mo_fsm_conf->choice  ==MAP_PROV_ERROR  )
				{

					if (give_trace)	printf("mo_fsm_conf->prov_error [%u]\n",mo_fsm_conf->confirm.prov_error);

				}
				else if (mo_fsm_conf->choice  ==MAP_USER_ERROR)
				{
					if (give_trace)	printf("mo_fsm_conf->user_error.ecode [%u]\n",mo_fsm_conf->confirm.user_error.error_code);


				}		

				break;

			}


		case MAP_N_PC_STATE_INDICATION:
			{
				map_n_pc_state_indication_t *p_n_pc_state_ind = NULL;

				printf("Received MAP_N_PC_STATE_INDICATION \n"); 
				p_n_pc_state_ind = (map_n_pc_state_indication_t *)p_api->p_data;
				printf("PC [%d] State [%d] \n",p_n_pc_state_ind->affected_spc,p_n_pc_state_ind->affected_spc_status);
			}
			break;

		case MAP_N_STATE_INDICATION :
			{
				map_n_state_indication_t *p_n_state_ind = NULL;

				printf("Received MAP_N_STATE_INDICATION \n"); 
				p_n_state_ind = (map_n_state_indication_t *)p_api->p_data;
				printf("SSN [%d] State [%d] \n",p_n_state_ind->affected_subsystem.ssn,p_n_state_ind->subsys_status);

			}
			break;

		case MAP_U_ABORT_INDICATION:
			{
				uaborts++;
			}
			break;

		case MAP_P_ABORT_INDICATION:
			{
				map_p_abort_indication_t	*p_abort;

				p_abort = (map_p_abort_indication_t *) p_api->p_data;

				printf("\n PABORT Reason [%d] SRC [%d]\n", p_abort->prov_reason,p_abort->src);
				paborts++;
			}
			break;


		default:
			printf("SOME OTHER API\n");
			break;

	}




	if ((mtfsm_v3_ind_c%50 ==0) && (mtfsm_v3_ind_c!=0) )
	{
		printf ("\n OPEN_IND [%d] MTFSM_IND [%d] CLOSE_REQ  [%d] UABORTS [%d] PABORTS [%d]  \n",(int) open_ind_c,(int)mtfsm_v3_ind_c,(int)close_req_c,(int) uaborts,(int) paborts);	

	}


	app_mem_free(p_api->p_data);
	app_mem_free(p_api);
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

#if 0
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

#endif
void send_mtfsm_v3_req(void)
{
		map_api_struct_t                   *p_send_api;
		map_open_request_t                 *open_req = NULL;
		map_mt_forward_sm_request_t      *mt_forw_sm_req = NULL;
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

