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


/* NexGen Specific Data Starts */
unsigned char			vlr_user_id = 7 ;
int  g_invoke_id =5;


#define SS7P_MAX_IPADDR_LEN 300
#define MAP_NUMBER_OF_HUNG_TCAP_DIALOGS 40000
#define SMSC_SSN			8
#define VLR_SSN				7	


int seq_control =1,flag_res=0;

unsigned long open_req_c=0, close_ind_c = 0,open_ind_c =0,close_req_c=0;

unsigned char origSSN = SMSC_SSN;
unsigned char g_sap = 1;
unsigned int g_spc = 1000;
unsigned int g_dpc = 2000;

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
unsigned long ind_counter=0;
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

int user_registered = 0;
int aborts = 0;
int uaborts = 0;
int paborts = 0;
/* NexGen Specific Data Ends */

/* USSD Application Specific Data Start*/
char temp_state[20] = {'\0'};
int temp_state_length = 0;

/* State and ussd_strings have one to one mapping */

char state [30][20] = {"*1234#"
			,"*1234*1#"
			,"*1234*1*1#"
			,"*1234*1*1**#"
			,"*1234*1*2#"
			,"*1234*1*2**#"
			,"*1234*1*3#"
			,"*1234*1*3**#"
			,"*1234*1**#"
			,"*1234*2#"
			,"*1234*2*1#"
			,"*1234*2*1**#"
			,"*1234*2*2#"
			,"*1234*2*2**#"
			,"*1234*2*3#"
			,"*1234*2*3**#"
			,"*1234*2*4#"
			,"*1234*2*4**#"
			,"*1234*2**#"
			,"*1234*3#"
			,"*1234*3*1#"
			,"*1234*3*1**#"
			,"*1234*3*2#"
			,"*1234*3*2**#"
			,"*1234*3*3#"
			,"*1234*3*3**#"
			,"*1234*3**#"
			,"*1234*4#"
			};

unsigned char ussd_strings [30][160] ={"Welcome to Aricent USSD Portal.\nPress 1 for Jokes\nPress 2 for Weather\nPress 3 for Sports\nPress 4 to Disconnect\n"
				,"Press 1 for Office Jokes\nPress 2 for Saas-Bahu Jokes\nPress 3 for Other Jokes\nPress * for main menu\n"
				,"Joke:\nWhen I told the doctor about my loss of memory, he made me pay in advance.\nPress * for previous menu\n"
				,""
				,"Joke:\nSaas-Bahu Jokes\nPress * for previous menu\n"
				,""
				,"Joke:\nThey call our language the mother tongue because the father seldom gets to speak.\nPress * for previous menu\n"
				,""
				,""
				,"Press 1 for North India\nPress 2 for South India\nPress 3 for West India\nPress 4 for East India\nPress * to for main menu\n"
				,"DELHI Temperature : Max 36 Min 27\nHumidity 64%\nPressure 1000mb\nVisibility Very Good\nPress * for previous menu\n"
				,""
				,"CHENNI Temperature : Max 40 Min 33\nHumidity 84%\nPressure 1005mb\nVisibility Very Good\nPress * for previous menu\n"
				,""
				,"MUMBAI Temperature : Max 32 Min 25\nHumidity 60%\nPressure 990mb\nVisibility Very Good\nPress * for previous menu\n"
				,""
				,"KOLKATA Temperature : Max 38 Min 29\nHumidity 70%\nPressure 1007mb\nVisibility Very Good\nPress * for previous menu\n"
				,""
				,""
				,"Press 1 for Cricket\nPress 2 for Tennis\nPress 3 for others\nPress * for main menu\n"
				,"PCB unhappy with ICC Task Force recommendations\nYuvraj Singh hungry for English outing\nPress * for previous menu\n"
				,""
				,"Serena Williams falls to 175 in latest rankings\nPress * for previous menu\n"
				,""
				,"Indian officials fired over doping cases\nDoping scandal: SAI sacks coaches\nPress * for previous menu\n"
				,""
				,""
				,"Invalid Response. Please Retry."
				}; 
/* USSD Application Specific Data End */


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

void map_fill_res_hdr_v2(map_service_header_t *resp_hdr, map_service_header_t sm_ind_hdr)
{
	resp_hdr->dlg_id = sm_ind_hdr.dlg_id;
	resp_hdr->is_corr_id = MAP_FALSE;
	resp_hdr->invoke_id = g_invoke_id++;
	resp_hdr->is_linked_id = MAP_FALSE;
	resp_hdr->last_component = MAP_TRUE ;
}


/*******************************************************************************
 ** FUNCTION NAME: register_user
 **
 ** DESCRIPTION:  register the USSD application with MAP Platform so that Application can use services of MAP Platform 
 **
 ** RETURNS:  void 
 **       
 ******************************************************************************/
void register_user(int l_ssn,int l_user_id,int sap,int spc)
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
	//p_send_api->header.spare1 = g_sap; /* TEMP */
	p_send_api->header.spare1 =sap; /* TEMP */
	/* send register request */
	reg_req = (map_app_register_user_request_t*)\
		  app_mem_get(sizeof(map_app_register_user_request_t));

	map_memzero(reg_req, sizeof(map_app_register_user_request_t));

	reg_req->user_id = l_user_id;
	reg_req->ssn = l_ssn;
	reg_req->is_supported_ac_list = 0;
	reg_req->supported_ac_list[1] = 1;
	//reg_req->sap_id = g_sap;
	reg_req->sap_id = sap;
	//reg_req->spc = g_spc;
	reg_req->spc = spc;

	p_send_api->header.api_id = MAP_REGISTER_USER_REQUEST;
	p_send_api->header.len = sizeof(map_app_register_user_request_t);
	p_send_api->p_data = reg_req;

	app_map_send_to_app_map((unsigned char *)p_send_api, &error);
	printf("Sent MAP_REGISTER_USER_REQUEST for SSN %d\n",reg_req->ssn);
}


/* Function to fill the parameters of MAP OPEN Response */
void fill_open_resp(map_open_response_t *open,map_open_indication_t *open_ind)
{
	open->dialog_id = open_ind->dialog_id;
	open->result = MAP_DIALOGUE_ACCEPTED;
	open->is_acn = MAP_TRUE;
	open->app_cont_name.appl_context = open_ind->app_cont_name.appl_context ;
	open->app_cont_name.version =  open_ind->app_cont_name.version;
	open->is_resp_add = MAP_FALSE;
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
	//open->is_qos = MAP_FALSE;
	open->is_qos = MAP_TRUE;
	open->qos.protocol_class = MAP_SCCP_PROTOCOL_CLASS_1;
	//open->qos.protocol_class = MAP_SCCP_PROTOCOL_CLASS_0;
	open->qos.return_option = MAP_SCCP_RETURN_ON_ERROR;

	if(seq_control == 14)
	{
		open->qos.sequence_control = 1;
		seq_control = 1;
	}
	else
	{
		open->qos.sequence_control = seq_control;
		seq_control++;
	}


	open->is_spec_info = open_ind->is_spec_info;
	open->is_ref_reason = MAP_FALSE;
}

/*Function to close USSD session*/
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
	send_api->header.spare1 = g_sap;
	send_api->p_data = close_req;
	app_map_send_to_app_map((unsigned char *)send_api, &error);


}


/******************************************************************
 *   * * Author(s)    :
 *      Function Name:encode_to_7bitformat
 *      Date         :02/08/2010
 *      parameters:
 *      ussdstring : The ussd string to be encoded
 *      encodedusssdstring : This is output parameter functions fills the 7 bit encoded value into this buffer
 *      length : This is length of ussdstring to be encoded
 *      Return value:
 *      It returns -1 if the ussdstring is NULL otherwise it returns the length of 7 bit encoded ussd string
 *      * *********************************************************************/

int encode_to_7bitformat( unsigned char *ussdstring,unsigned char *encodedusssdstring,int length)
{
	int j = 0,nBits = 1, i = 0;
	if(ussdstring == NULL)
	{
		return -1;
	}
	for( i = 0;i < length;)
	{
		nBits = 1;
		encodedusssdstring[j] = ussdstring[i];
		i++;
		for(;i<=length&&nBits != 8;i++,j++,nBits++)
		{
			encodedusssdstring[j] =  encodedusssdstring[j]|(ussdstring[i]<<(8-nBits));
			ussdstring[i] = ussdstring[i]>>nBits;
			encodedusssdstring[j+1] = ussdstring[i];
		}
	}
	if((j % 7)  == 0) //if length is modules of 7 then append the carriage retuen
	{
		encodedusssdstring[j - 1] =  encodedusssdstring[j -1] | 0x1A; // appending the carriage return according to 6.1.2.3.1 3GPP
	}
	return j;
}

/******************************************************************
 *   * * Author(s)    :
 *      Function Name:decode_to_8bitformat
 *      Date         :02/08/2010
 *      parameters:
 *      encodedusssdstring : The ussd string to be decoded
 *      ussdstring : This is output parameter functions fills the 8 bit decoded value into this buffer
 *      length : This is length of ussdstring to be encoded
 *      Return value:
 *      It returns -1 if the ussdstring is NULL otherwise it returns the length of 8 bit decoded ussd string
 *      * *********************************************************************/

int decode_to_8bitformat(unsigned char *encodedusssdstring, unsigned char *ussdstring,int length)
{
	int i= 0,counter = 0,j=0,length_string = 0;
	if (encodedusssdstring == NULL)
	{
		return -1;
	}
	unsigned char temp = 0;
	while(j<=length)
	{
		temp = 0;
		for( i = 0; i < 7 && j <= length;i++ )
		{
			*ussdstring = ((encodedusssdstring[j]<<i) | temp ) & 0x7f;
			length_string++;
			temp = encodedusssdstring[j] >> (7-i);
			ussdstring++;
			j++;
			if(i == 6)
			{
				length_string++;
				*ussdstring = temp & 0x7f;
				ussdstring++;
			}

		}
	}
	if(length >= 7 )
		return ++length;
	return length;
}


/*Function to send USSD Request to User prompting him to send back the option*/
void send_v2_process_unstructured_ss_req_resp(int user_id,int version,map_service_header_t mo_forw_sm_ind_hdr,int last_flag,int index)
{
	int error =0;
	map_v2_unstructured_ss_request_request_t *mo_fsm_res = NULL;
	map_api_struct_t *send_api = NULL;
	unsigned char ussd_string_ussr[160] =  {'\0',} ;
	unsigned char encodedusssdstring[160] = {'\0',};
	
	/*Allocate memory for USSD Request*/
        send_api = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
	map_memzero(send_api,sizeof(map_api_struct_t));
	mo_fsm_res = (map_v2_unstructured_ss_request_request_t *)app_mem_get(sizeof(map_v2_unstructured_ss_request_request_t));
	map_memzero(mo_fsm_res,sizeof(map_v2_unstructured_ss_request_request_t));
	fill_api_header(&(send_api->header),user_id);
	map_fill_res_hdr_v2(&(mo_fsm_res->header),mo_forw_sm_ind_hdr);
	mo_fsm_res->header.last_component = last_flag;

	/*Fill USSD parameters*/
        mo_fsm_res->arg.ussd_data_coding_scheme.length = 1;
	mo_fsm_res->arg.ussd_data_coding_scheme.value[0] = 0x0f;
	memcpy(ussd_string_ussr,ussd_strings[index],strlen(ussd_strings[index]));
	int length1 =  encode_to_7bitformat(ussd_string_ussr,encodedusssdstring, strlen(ussd_strings[index]));

	mo_fsm_res->arg.ussd_string.length =   length1;
	memcpy(mo_fsm_res->arg.ussd_string.value,encodedusssdstring,length1);

	send_api->header.api_id = MAP_UNSTRUCTURED_SS_REQUEST_REQUEST;
	send_api->header.len = sizeof(map_v2_unstructured_ss_request_request_t);
	send_api->header.ver = version;
	send_api->header.spare1 = g_sap;
	send_api->p_data = mo_fsm_res;

        /*Send the USSD request to MAP platform*/ 
	app_map_send_to_app_map((unsigned char *)send_api, &error);
}




/*******************************************************************************
 ** FUNCTION NAME: app_map_send_to_app_map_user
 **
 ** DESCRIPTION: Call back function. This function is scheduled whenever data comes from MAP Platform.  
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


	switch(p_api->header.api_id)
	{
		case  MAP_APP_SAP_SSN_STATE_INDICATION:
			{
				map_app_sap_ssn_state_indication_t  *open_ind;
				open_ind = (map_app_sap_ssn_state_indication_t *)p_api->p_data;
				printf("Received SAP STATE IND state [%d] SSN [%d] SAP ID [%d]\n", open_ind->state, open_ind->ssn, open_ind->sap_id);

				if(open_ind->state == 1)
				{

					register_user(147,vlr_user_id,1,1000);
				}

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


				/* Changes  Start */
				send_api = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
				map_memzero(send_api,sizeof(map_api_struct_t));

				fill_api_header(&(send_api->header),p_api->header.user_id);
				/* Changes  End */


				//printf("Received MAP_OPEN_INDICATION \n");
				open_ind_c++;	
				/*Prepare the MAP OPEN Response */
				open_ind = (map_open_indication_t *)p_api->p_data;
				open_resp = (map_open_response_t *)app_mem_get(sizeof(map_open_response_t));
				map_memzero(open_resp,sizeof(map_open_response_t));

				fill_open_resp(open_resp,open_ind);

				open_resp->resp_add.is_ssn = 1;
				//open_resp->resp_add.ssn = origSSN; /* GSM SCF */
				open_resp->resp_add.ssn = VLR_SSN; /* GSM SCF */

				open_resp->resp_add.routing_ind = ROUTE_ON_SSN;
				open_resp->resp_add.is_spc=1;
				open_resp->resp_add.is_gt=0;
				send_api->header.api_id = MAP_OPEN_RESPONSE;
				send_api->header.len = sizeof(map_open_response_t);
				send_api->header.ver =  p_api->header.ver ;
				/* Fill the SAP in spare 1*/
				//send_api->header.spare1 = 1;
				send_api->header.spare1 = g_sap ;

				send_api->p_data = open_resp;

				/*send the open response */
				if(app_map_send_to_app_map((unsigned char *)send_api, &error) == APP_FAILURE)
				{
					//	printf("ERROR IN SENDING.....ECODE=%d\n",error);
				}

			}
			break;


		case MAP_DELIMITER_INDICATION:
			{

				//printf("####DELIMITER indication has Recieved \n");
				map_delimiter_indication_t *delim_ind = NULL;

				if(send_api !=NULL)
					app_mem_free(send_api);
				delim_ind = (map_delimiter_indication_t *)p_api->p_data;
			}
			break;




		case MAP_PROCESS_UNSTRUCTURED_SS_REQUEST_INDICATION:
			{
				/* Received Short Code from user */
				if(p_api->header.ver == 2)			
				{
					int length = 0,i = 0;
					char ussd_string_8bit[160] = {'\0'};
					printf("MAP_PROCESS_UNSTRUCTURED_SS_REQUEST_INDICATION indication recieved for Ver 2\n");
					map_v2_process_unstructured_ss_request_indication_t *p_rout_ind = NULL;
					p_rout_ind = (map_v2_process_unstructured_ss_request_indication_t *)p_api->p_data;
						
					length = decode_to_8bitformat(p_rout_ind->arg.ussd_string.value,ussd_string_8bit,p_rout_ind->arg.ussd_string.length);
					printf("USSD String Received [%s][%d]\n",ussd_string_8bit,length );
					printf("MAP_PROCESS_UNSTRUCTURED_SS_REQUEST_INDICATION indication recieved for Ver 2\n");

					for (i  = 0; i < 30; i++)
					{
						if (strncmp(ussd_string_8bit,state[i],length) == 0) /* Compare with stored Short code */
						{
							/* Sending corresponding USSR srting to user */
							send_v2_process_unstructured_ss_req_resp(p_api->header.user_id,p_api->header.ver,p_rout_ind->header,1,i);
							temp_state_length = length;
							strncpy(temp_state,ussd_string_8bit,length);
							break;
						}
					}
					/* No Match Found , Clode Dialog */
					if(i == 30)
					{
						temp_state_length = 0;
						temp_state[0] = '\0';
						send_v2_process_unstructured_ss_req_resp(p_api->header.user_id,p_api->header.ver,p_rout_ind->header,1,27);
						send_close_req(p_api->header.user_id,p_rout_ind->header.dlg_id,p_api->header.ver);

					}
				}
			}
			break;

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
		case MAP_UNSTRUCTURED_SS_REQUEST_CONFIRM:/*USSD Resonse (Option) recieved from User*/
			{        
				if(p_api->header.ver == 2)			
				{
					int length = 0,i = 0;
					char ussd_string_8bit[160] = {'\0'};
					printf("MAP_UNSTRUCTURED_SS_REQUEST_CONFIRM recieved for Ver 2\n");
					map_v2_unstructured_ss_request_confirm_t *p_rout_ind = NULL;
					p_rout_ind = (map_v2_unstructured_ss_request_confirm_t *)p_api->p_data;
					length = decode_to_8bitformat(p_rout_ind->confirm.result.ussd_string.value,ussd_string_8bit,p_rout_ind->confirm.result.ussd_string.length);
					temp_state[temp_state_length - 1] = '*';
					temp_state[temp_state_length] = '\0';
					strncat(temp_state,ussd_string_8bit,length);
					temp_state[temp_state_length + length] = '#';
					temp_state_length = temp_state_length +  length + 1;		
					temp_state[temp_state_length] = '\0';
					printf("USSD String Received [%s][%d]\n",temp_state,temp_state_length );
					for (i  = 0; i < 30; i++)
					{
						if (strncmp(temp_state,state[i],temp_state_length) == 0)
						{
							switch (i)
							{
								case 3:
								case 5:
								case 7:
									/* Going Previous Menu "Joke" */
									temp_state_length = 8;
									temp_state[temp_state_length - 1] = '#';
									temp_state[temp_state_length] = '\0';
									send_v2_process_unstructured_ss_req_resp(p_api->header.user_id,p_api->header.ver,p_rout_ind->header,1,1);

									break;

								case 11:
								case 13:
								case 15:
								case 17:
									/* Going Previous Menu "Weather" */
									temp_state_length = 8;
									temp_state[temp_state_length - 1] = '#';
									temp_state[temp_state_length] = '\0';
									send_v2_process_unstructured_ss_req_resp(p_api->header.user_id,p_api->header.ver,p_rout_ind->header,1,9);

									break;

								case 21:
								case 23:
								case 25:
									/* Going Previous Menu "Sport" */
									temp_state_length = 8;
									temp_state[temp_state_length - 1] = '#';
									temp_state[temp_state_length] = '\0';
									send_v2_process_unstructured_ss_req_resp(p_api->header.user_id,p_api->header.ver,p_rout_ind->header,1,19);

									break;
								case 8:
								case 18:
								case 26:
									/* Going Previous Menu "Main" */
									temp_state_length = 6;
									temp_state[temp_state_length - 1] = '#';
									temp_state[temp_state_length] = '\0';
									send_v2_process_unstructured_ss_req_resp(p_api->header.user_id,p_api->header.ver,p_rout_ind->header,1,0);

									break;
								case 27:
									/* Received Disconnect option from user */
									temp_state[0] = '\0';
									temp_state_length = 0;
									send_close_req(p_api->header.user_id,p_rout_ind->header.dlg_id,p_api->header.ver);
									break;
								default :
									/* Going Forward in menu as per user choice */
									send_v2_process_unstructured_ss_req_resp(p_api->header.user_id,p_api->header.ver,p_rout_ind->header,1,i);
									break;		 
							}
							printf("USSD String Received [%s][%d]\n",temp_state,temp_state_length );
							break;
						}
					}
					/* No Match Found , Clode Dialog */
					if(i == 30)
					{
						temp_state_length = 0;
						temp_state[0] = '\0';
						send_v2_process_unstructured_ss_req_resp(p_api->header.user_id,p_api->header.ver,p_rout_ind->header,1,27);
						send_close_req(p_api->header.user_id,p_rout_ind->header.dlg_id,p_api->header.ver);
					}
				}
			}
			break;

		default:
			printf("SOME OTHER API\n");
			break;

	}

	app_mem_free(p_api->p_data);
	app_mem_free(p_api);
	return APP_SUCCESS;
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

	i = 1;	


	t_flag=i_flag = a_flag = s_flag = m_flag = v_flag = options_flag = APP_FALSE; 
	a_em_complete = s_em_complete = APP_FALSE;
	if ( (p_current_dir = (char *)getenv("PWD")) == NULL)   
	{
		printf("EM :: Failed to get the ENVIRONMENT VARIABLE PWD\n");
		exit(0);
	}

	/* Reading Command line argument. Required for application start up  Start */
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
	/* Reading Command line argument. Required for application start up  End */
	app_map_self.port_trace = 0;
	
	while (1)
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
		if (APP_FAILURE == app_map_schedule(num_selected_fd,(&map_readfd),(&map_writefd),(&map_exceptfd), &ss7p_suggested_select_timeout, &ecode))
		{
			printf("\n app_map_schedule Failed \n");
			exit(0);
		}

	}
	return(1);
}


