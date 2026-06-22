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
//unsigned char			msc_user_id = 5;
unsigned char			user_id = 8;
unsigned char			vlr_user_id = 7;
unsigned char			gsmscf_user_id = 1;
unsigned char			hlr_user_id = 6;


int give_trace = 0,global_corr_id = 0,seq_control=1;

#define SS7P_MAX_IPADDR_LEN 300
#define MAP_NUMBER_OF_HUNG_TCAP_DIALOGS 40000
#define SMSC_SSN			8
#define MSC_SSN			8
#define GMSC_SSN			8
#define HLR_SSN				6	
#define VLR_SSN				7	
#define EIR_SSN				9	

time_t g_current_time,g_initial_time;

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
unsigned int g_spc = 1000;
unsigned int g_dpc = 2000;


/* Imran Lateef Start Declaration */
map_api_struct_t  *send_api_mofsm = NULL,*send_api_temp = NULL,*send_api=NULL,*send_api_mtfsm = NULL,*send_api_sri = NULL,*send_api_srism = NULL,*send_api_lu=NULL,*send_api_sri_temp = NULL,*send_api_srism_temp = NULL,*send_api_lu_temp=NULL,*send_api_ussr=NULL,*send_api_ussr_temp=NULL,*send_api_purgems=NULL;
map_mo_forward_sm_request_t     *p_v1_fsm_req = NULL,*p_v1_fsm_req_temp = NULL;
map_mt_forward_sm_request_t	*p_mt_fsm_req = NULL,*p_mt_fsm_req_temp = NULL;
map_send_routing_info_request_t *p_send_routing_info_req = NULL,*p_send_routing_info_req_temp = NULL;
map_routing_info_for_sm_request_t *p_send_routing_info_sm_req = NULL,*p_send_routing_info_sm_req_temp = NULL;
map_update_location_request_t *p_update_loc_req = NULL,*p_update_loc_req_temp = NULL;
map_v1_update_location_request_t *p_update_loc_req_1 = NULL,*p_update_loc_req_temp_1 = NULL;
map_v2_unstructured_ss_request_request_t *p_v2_unstructured_ss_data_req = NULL,*p_v2_unstructured_ss_data_req_temp = NULL;
map_v2_purge_ms_request_t *p_v2_purgems_req=NULL;




/* Imran Lateef End  Declaration */
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

/* Soak Counters */
unsigned long  ussd_request_counters =0;
unsigned long  ussd_confirm_counters =0;
unsigned long  ussd_failure_counters =0;
unsigned long  local_cacel_counters =0;
unsigned long  mt_confirm_counters =0;
unsigned long  mo_confirm_counters =0;
unsigned long  mt_request_counters =0;
unsigned long  mo_request_counters =0;
unsigned long  sri_request_counters =0;
unsigned long  srism_request_counters =0;
unsigned long  lu_request_counters =0;
unsigned long  sri_confirm_counters =0;
unsigned long  srism_confirm_counters =0;
unsigned long  lu_confirm_counters =0;

void sigusr2_hdlr(int sig1);



//Imran Doing work Start

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
	int no_message_in_burst; 
	int delay_bet_burst; 
	//int timer_duration; 
}app_map_apis_t;

typedef struct app_map_scenario{

	int is_same_dialogue;
	int dialogue_id;
	int number_apis;
	int cycles; 
	app_map_apis_t *p_app_map_apis;
	
}app_map_scenarios_t;


typedef enum
{

    MO_FORWARD_SHORT_MESSAGE_REQUEST =0,
    MT_FORWARD_SHORT_MESSAGE_REQUEST,
    MAP_V2_FORWARD_SHORT_MESSAGE,
    MAP_V1_ALERT_SERVICE_CENTRE,
    MAP_V2_ALERT_SERVICE_CENTRE,
    MAP_V2_PROCESS_USSR,
    MAP_V2_USSR,
    MAP_V2_USSN,
    MAP_V1_CHECK_IMEI,
    MAP_V2_CHECK_IMEI,
    MAP_V3_CHECK_IMEI,
    MAP_V2_ISC,
    MAP_V3_ISC,
    MAP_V1_SRI,
    MAP_V2_SRI,
    MAP_V3_SRI,
    MAP_V1_SRI_SM,
    MAP_V2_SRI_SM,
    MAP_V3_SRI_SM,
    MAP_V2_REPORT_SM_STATUS,
    MAP_V3_REPORT_SM_STATUS,
    MAP_V1_PRN,
    MAP_V2_PRN,
    MAP_V3_PRN,
    MAP_V3_SET_REPORT_STATE,
    MAP_V3_REMOTE_USER_FREE,
    MAP_V3_STATUS_REPORT,
    MAP_V3_IST_ALERT,
    MAP_V3_IST_COMMAND,
    MAP_V3_RESTORE_DATA,
    MAP_V3_ISD

}enum_val;


typedef struct api_data{

	int request_count;
	int indication_count;
	int response_count;
	int confirm_count;
}api_data_t;

typedef struct performance_data{
	
	api_data_t mo_forward_sm;	
	api_data_t mt_forward_sm;
	api_data_t map_v2_forward_sm;
	api_data_t map_v1_alert_sc;
	api_data_t map_v2_alert_sc;
	api_data_t map_v2_process_uss_req;
	api_data_t map_v2_unstructured_ss_req;
	api_data_t map_v2_unstructured_ss_notify;
	api_data_t map_v1_check_imei;
	api_data_t map_v2_check_imei;
	api_data_t map_v3_check_imei;
	api_data_t map_v2_isc;
	api_data_t map_v3_isc;
	api_data_t map_v1_sri_req;
	api_data_t map_v2_sri_req;
	api_data_t map_v3_sri_req;
	api_data_t map_v1_rout_info_for_sms;
	api_data_t map_v2_rout_info_for_sms;
	api_data_t map_v3_rout_info_for_sms;
	api_data_t map_v2_report_sm_status;
	api_data_t map_v3_report_sm_status;
	api_data_t map_v1_prn_req;
	api_data_t map_v2_prn_req;
	api_data_t map_v3_prn_req;
	api_data_t map_v3_set_report_state_req;
	api_data_t map_v3_remote_user_free;
	api_data_t map_v3_status_report_req;
	api_data_t map_v3_ist_alert_req;
	api_data_t map_v3_ist_command_req;
	api_data_t map_v3_restore_data;
	api_data_t map_v3_isd_req;
}performance_data_t;


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
  //char *s = malloc(10) ;
  char *s = NULL ;
  int result = 0;
  int firsttime = 1;
  int found = 0;
  int i = 0;

  //strcpy(s,value);
  s=value ;
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

 // free(s);	 //Purify 
  return result;
}


void fill_isd_req_argument (map_insert_subscriber_data_request_t *isd_req)
{
	memset(&isd_req->arg, 0, sizeof(map_insert_subscriber_data_arg_t));
	isd_req->arg.is_imsi = 1;
	isd_req->arg.imsi.length = 4;
	isd_req->arg.imsi.value[0] = 0x11;
	isd_req->arg.imsi.value[1] = 0x11;
	isd_req->arg.imsi.value[2] = 0x11;
	isd_req->arg.imsi.value[3] = 0x11;

	isd_req->arg.is_msisdn = 1;
	isd_req->arg.msisdn.length = 5; 
	isd_req->arg.msisdn.value[0] = 0x91;
	isd_req->arg.msisdn.value[1] = 0x36;
	isd_req->arg.msisdn.value[2] = 0x19;
	isd_req->arg.msisdn.value[3] = 0x66;
	isd_req->arg.msisdn.value[4] = 0x55;

	isd_req->arg.is_category = 1;
	isd_req->arg.category.length = 1;
	isd_req->arg.category.value[0] = 0x11;

	isd_req->arg.is_subscriber_status = 1;
	isd_req->arg.subscriber_status = 1;

	isd_req->arg.is_bearer_service_list = 1;
	isd_req->arg.bearer_service_list.count = 1;
	isd_req->arg.bearer_service_list.value[0].length = 5;
	isd_req->arg.bearer_service_list.value[0].value[0] = 0x33;
	isd_req->arg.bearer_service_list.value[0].value[1] = 0x33;
	isd_req->arg.bearer_service_list.value[0].value[2] = 0x33;
	isd_req->arg.bearer_service_list.value[0].value[3] = 0x33;
	isd_req->arg.bearer_service_list.value[0].value[4] = 0x33;

	isd_req->arg.is_teleservice_list = 1;
	isd_req->arg.teleservice_list.count = 3;
	isd_req->arg.teleservice_list.value[0].length = 1;
	isd_req->arg.teleservice_list.value[0].value[0] = 0x11;
	isd_req->arg.teleservice_list.value[1].length = 1;
	isd_req->arg.teleservice_list.value[1].value[0] = 0x11;
	isd_req->arg.teleservice_list.value[2].length = 1;
	isd_req->arg.teleservice_list.value[2].value[0] = 0x11;

	//isd_req->arg.is_provisioned_ss = 1;
	isd_req->arg.is_provisioned_ss = 0;
	isd_req->arg.is_odb_data = 0;
	isd_req->arg.is_roaming_restriction_due_to_unsupported_feature = 0;
	isd_req->arg.is_regional_subscription_data = 0;
	isd_req->arg.is_vbs_subscription_data = 0;
	isd_req->arg.is_vgcs_subscription_data = 0;
	isd_req->arg.is_vlr_camel_subscription_info = 0;
	isd_req->arg.is_extension = 0;
	isd_req->arg.is_naea_preferred_ci = 0;
	isd_req->arg.is_gprs_subscription_data = 0;
	isd_req->arg.is_roaming_restricted_in_sgsn_due_to_unsupported_feature= 0;
	isd_req->arg.is_network_access_mode = 0;
	isd_req->arg.is_lsa_information = 0;
	isd_req->arg.is_lmu_indicator = 0;
	isd_req->arg.is_lcs_information = 0;
	isd_req->arg.is_ist_alert_timer = 0;
	isd_req->arg.is_super_charger_supported_in_hlr = 0;
	isd_req->arg.is_mc_ss_info = 0;
	isd_req->arg.is_cs_allocation_retention_priority = 0;
	isd_req->arg.is_sgsn_camel_subscription_info = 0;
	isd_req->arg.is_charging_characteristics = 0;

#if 0
	isd_req->arg.provisioned_ss.count = 4;
	isd_req->arg.provisioned_ss.value[0].choice = 4;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.ss_code.length= 1;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.ss_code.value[0]= 0x42;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.ss_status.length= 3;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.ss_status.value[0]= 0x05;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.ss_status.value[1]= 0x23;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.ss_status.value[2]= 0x31;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.is_ss_subscription_option= 1;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.ss_subscription_option.choice = MAP_OVERRIDE_CATEGORY_CHOSEN; //=2
	isd_req->arg.provisioned_ss.value[0].u.ss_data.ss_subscription_option.u.override_category = MAP_OVERRIDE_DISABLED; //=1
	isd_req->arg.provisioned_ss.value[0].u.ss_data.is_basic_service_group_list = 1;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.basic_service_group_list.count = 1;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.basic_service_group_list.value[0].choice = 1;
						/*MAP_EXT_BASIC_SERVICE_CODE_EXT_BEARER_SERVICE_CHOSEN; //=1*/
	isd_req->arg.provisioned_ss.value[0].u.ss_data.basic_service_group_list.value[0].u.ext_bearer_service.length = 2;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.basic_service_group_list.value[0].u.ext_bearer_service.value[0] = 0x21;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.basic_service_group_list.value[0].u.ext_bearer_service.value[1] = 0x43;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.is_extension= 1;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.map_pvt_ext_count = 1;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].ext_obj_id.length = 9;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].ext_obj_id.value[0] = 0x2a;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].ext_obj_id.value[1] = 0x07;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].ext_obj_id.value[2] = 0x3a;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].ext_obj_id.value[3] = 0x00;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].ext_obj_id.value[4] = 0x89;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].ext_obj_id.value[5] = 0x61;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].ext_obj_id.value[6] = 0x3a;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].ext_obj_id.value[7] = 0x01;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].ext_obj_id.value[8] = 0x00;

	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].is_ext_type = 1;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].enc_len = 9;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].encoded[0] = 0xa7;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].encoded[1] = 0x07;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].encoded[2] = 0x30;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].encoded[3] = 0x05;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].encoded[4] = 0x81;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].encoded[5] = 0x01;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].encoded[6] = 0x0e;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].encoded[7] = 0x82;
	isd_req->arg.provisioned_ss.value[0].u.ss_data.extension_container.private_ext_list[0].encoded[8] = 0x00;

	isd_req->arg.provisioned_ss.value[1].choice = MAP_EXT_SS_INFO_FORWARDING_INFO_CHOSEN;//=1
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.ss_code.length=1;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.ss_code.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.count=1;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].is_basic_service=1;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].basic_service.choice=2;
	/*MAP_EXT_BASIC_SERVICE_CODE_EXT_TELESERVICE_CHOSEN;//=2*/
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].basic_service.u.ext_teleservice.length=4;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].basic_service.u.ext_teleservice.value[0]=0x44;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].basic_service.u.ext_teleservice.value[1]=0x44;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].basic_service.u.ext_teleservice.value[2]=0x44;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].basic_service.u.ext_teleservice.value[3]=0x44;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].ss_status.length=4;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].ss_status.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].ss_status.value[1]=0x22;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].ss_status.value[2]=0x22;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].ss_status.value[3]=0x22;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].is_forwarded_to_number=1;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].forwarded_to_number.length=4;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].forwarded_to_number.value[0]=0x33;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].forwarded_to_number.value[1]=0x33;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].forwarded_to_number.value[2]=0x33;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].forwarded_to_number.value[3]=0x33;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].is_forwarded_to_subaddress=0;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].forwarded_to_subaddress.length=1;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].forwarded_to_subaddress.value[0]=0x11;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].is_forwarding_options=0;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].forwarding_options.length=1;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].forwarding_options.value[0]=0x11;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].is_no_reply_condition_time=0;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].no_reply_condition_time=0xaabb;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].is_extension=0;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].is_long_forwarded_to_number=0;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].long_forwarded_to_number.length=1;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.forwarding_feature_list.value[0].long_forwarded_to_number.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[1].u.forwarding_info.is_extension=0;

	isd_req->arg.provisioned_ss.value[2].choice = MAP_EXT_SS_INFO_CALL_BARRING_INFO_CHOSEN;//=2
	isd_req->arg.provisioned_ss.value[2].u.call_barring_info.ss_code.length=1;
	isd_req->arg.provisioned_ss.value[2].u.call_barring_info.ss_code.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[2].u.call_barring_info.call_barring_feature_list.count=1;
	isd_req->arg.provisioned_ss.value[2].u.call_barring_info.call_barring_feature_list.value[0].is_basic_service=0;
	isd_req->arg.provisioned_ss.value[2].u.call_barring_info.call_barring_feature_list.value[0].ss_status.length=1;
	isd_req->arg.provisioned_ss.value[2].u.call_barring_info.call_barring_feature_list.value[0].ss_status.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[2].u.call_barring_info.call_barring_feature_list.value[0].is_extension=0;
	isd_req->arg.provisioned_ss.value[2].u.call_barring_info.is_extension=0;

	isd_req->arg.provisioned_ss.value[3].choice = MAP_CUG_INFO_CHOSEN;//=3
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.count=8;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[0].cug_index=0x00bb;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[0].cug_interlock.length=4;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[0].cug_interlock.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[0].cug_interlock.value[1]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[0].cug_interlock.value[2]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[0].cug_interlock.value[3]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[0].intra_cug_options=MAP_NO_CUG_RESTRICTIONS;//=0
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[0].is_basic_service_group_list=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[0].is_extension=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[1].cug_index=0x00bb;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[1].cug_interlock.length=4;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[1].cug_interlock.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[1].cug_interlock.value[1]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[1].cug_interlock.value[2]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[1].cug_interlock.value[3]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[1].intra_cug_options=MAP_NO_CUG_RESTRICTIONS;//=0
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[1].is_basic_service_group_list=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[1].is_extension=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[2].cug_index=0x00bb;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[2].cug_interlock.length=4;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[2].cug_interlock.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[2].cug_interlock.value[1]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[2].cug_interlock.value[2]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[2].cug_interlock.value[3]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[2].intra_cug_options=MAP_NO_CUG_RESTRICTIONS;//=0
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[2].is_basic_service_group_list=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[2].is_extension=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[3].cug_index=0x00bb;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[3].cug_interlock.length=4;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[3].cug_interlock.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[3].cug_interlock.value[1]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[3].cug_interlock.value[2]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[3].cug_interlock.value[3]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[3].intra_cug_options=MAP_NO_CUG_RESTRICTIONS;//=0
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[3].is_basic_service_group_list=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[3].is_extension=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[4].cug_index=0x00bb;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[4].cug_interlock.length=4;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[4].cug_interlock.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[4].cug_interlock.value[1]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[4].cug_interlock.value[2]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[4].cug_interlock.value[3]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[4].intra_cug_options=MAP_NO_CUG_RESTRICTIONS;//=0
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[4].is_basic_service_group_list=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[4].is_extension=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[5].cug_index=0x00bb;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[5].cug_interlock.length=4;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[5].cug_interlock.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[5].cug_interlock.value[1]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[5].cug_interlock.value[2]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[5].cug_interlock.value[3]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[5].intra_cug_options=MAP_NO_CUG_RESTRICTIONS;//=0
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[5].is_basic_service_group_list=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[5].is_extension=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[6].cug_index=0x00bb;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[6].cug_interlock.length=4;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[6].cug_interlock.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[6].cug_interlock.value[1]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[6].cug_interlock.value[2]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[6].cug_interlock.value[3]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[6].intra_cug_options=MAP_NO_CUG_RESTRICTIONS;//=0
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[6].is_basic_service_group_list=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[6].is_extension=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[7].cug_index=0x00bb;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[7].cug_interlock.length=4;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[7].cug_interlock.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[7].cug_interlock.value[1]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[7].cug_interlock.value[2]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[7].cug_interlock.value[3]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[7].intra_cug_options=MAP_NO_CUG_RESTRICTIONS;//=0
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[7].is_basic_service_group_list=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_subscription_list.value[7].is_extension=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.is_cug_feature_list=1;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_feature_list.count=1;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_feature_list.value[0].is_basic_service=1;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_feature_list.value[0].basic_service.choice=2; /*MAP_EXT_BASIC_SERVICE_CODE_EXT_TELESERVICE_CHOSEN;//=2*/
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_feature_list.value[0].basic_service.u.ext_teleservice.length=1;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_feature_list.value[0].basic_service.u.ext_teleservice.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_feature_list.value[0].is_preferential_cug_indicator=1;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_feature_list.value[0].preferential_cug_indicator=0x3355;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_feature_list.value[0].inter_cug_restrictions.length=1;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_feature_list.value[0].inter_cug_restrictions.value[0]=0x22;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.cug_feature_list.value[0].is_extension=0;
	isd_req->arg.provisioned_ss.value[3].u.cug_info.is_extension=0;

#endif

}

void fill_sri_for_sm_req_argument (map_routing_info_for_sm_request_t *sri_for_sm_req)
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


void sigusr1_hdlr(int sig)
{
		signal (SIGUSR1, sigusr1_hdlr);
		signal (SIGUSR2, sigusr2_hdlr);
		fflush(stdout); fflush(stdin);

		choice = 1;
		msg_to_send = 1;
		return;
}

void sigusr2_hdlr(int sig1)
{

}

void print_ussd_report()
{
	FILE *fp_apis =NULL;
	time_t tval;
	char filename[100] = "./REPORTS";
	time(&tval);
	time(&g_current_time);
	signal (SIGUSR1, sigusr1_hdlr);
	signal (SIGUSR2, sigusr2_hdlr);

	fflush(stdout); fflush(stdin);

	fp_apis = fopen(filename,"a+");
	if(fp_apis == NULL)
	{
		printf("File Not Writable hence returning\n");
		return;
	}

	
	aborts = paborts + uaborts;
	fprintf(fp_apis,"###########################################\n");
	fprintf(fp_apis,"Time %s \n USSD Requests [%lu] USSD Confirms [%lu] ABORTS [%d] Local Cancellation [%lu] and Message Rate is [%d] \n",ctime(&tval),ussd_request_counters,ussd_confirm_counters,aborts,local_cacel_counters,ussd_request_counters/(g_current_time - g_initial_time));
	fprintf(fp_apis,"###########################################\n");

	fclose(fp_apis);

		
}


void print_mt_report()
{
	FILE *fp_apis =NULL;
	time_t tval;
	char filename[100] = "./REPORTS";
	time(&tval);
	time(&g_current_time);
	signal (SIGUSR1, sigusr1_hdlr);
	signal (SIGUSR2, sigusr2_hdlr);

	fflush(stdout); fflush(stdin);

	fp_apis = fopen(filename,"a+");
	if(fp_apis == NULL)
	{
		printf("File Not Writable hence returning\n");
		return;
	}

	
	aborts = paborts + uaborts;
	fprintf(fp_apis,"###########################################\n");
	fprintf(fp_apis,"Time %s \n MT-FSM-REQUEST [%lu] MT-FSM-CONFIRM Confirms [%lu] ABORTS [%d] Local Cancellation [%lu] Message Rate [%d]\n",ctime(&tval),mt_request_counters,mt_confirm_counters,aborts,local_cacel_counters,mt_request_counters/(g_current_time - g_initial_time));

	fprintf(fp_apis,"###########################################\n");

	fclose(fp_apis);

		
}



void print_mt_mo_report()
{
	FILE *fp_apis =NULL;
	time_t tval;
	char filename[100] = "./REPORTS";
	time(&tval);
	signal (SIGUSR1, sigusr1_hdlr);
	signal (SIGUSR2, sigusr2_hdlr);

	fflush(stdout); fflush(stdin);

	fp_apis = fopen(filename,"a+");
	if(fp_apis == NULL)
	{
		printf("File Not Writable hence returning\n");
		return;
	}

	
	aborts = paborts + uaborts;
	fprintf(fp_apis,"###########################################\n");
	fprintf(fp_apis,"Time %s \n MT-FSM-REQUEST [%lu] MT-FSM-CONFIRM Confirms [%lu] \n  MO-FSM-REQUEST [%lu] MO-FSM-CONFIRM Confirms [%lu] \n ABORTS [%d] Local Cancellation [%lu] \n",ctime(&tval),mt_request_counters,mt_confirm_counters,mo_request_counters,mo_confirm_counters,aborts,local_cacel_counters);

	fprintf(fp_apis,"###########################################\n");

	fclose(fp_apis);

		
}

void print_srism()
{
	FILE *fp_apis =NULL;
	time_t tval;
	char filename[100] = "./REPORTS";
	time(&tval);
	time(&g_current_time);
	signal (SIGUSR1, sigusr1_hdlr);
	signal (SIGUSR2, sigusr2_hdlr);

	fflush(stdout); fflush(stdin);

	fp_apis = fopen(filename,"a+");
	if(fp_apis == NULL)
	{
		printf("File Not Writable hence returning\n");
		return;
	}

	
	aborts = paborts + uaborts;
	fprintf(fp_apis,"###########################################\n");
	fprintf(fp_apis,"Time %s \n SRI-SM-REQUEST [%lu] SRI-SM-CONFIRM Confirms [%lu]  ABORTS [%d] Local Cancellation [%lu] Message Rate [%d]\n",ctime(&tval),srism_request_counters,srism_confirm_counters,aborts,local_cacel_counters,srism_request_counters/(g_current_time - g_initial_time));

	fprintf(fp_apis,"###########################################\n");

	fclose(fp_apis);

		
}


void print_srism_ussr()
{
	FILE *fp_apis =NULL;
	time_t tval;
	char filename[100] = "./REPORTS";
	time(&tval);
	signal (SIGUSR1, sigusr1_hdlr);
	signal (SIGUSR2, sigusr2_hdlr);

	fflush(stdout); fflush(stdin);

	fp_apis = fopen(filename,"a+");
	if(fp_apis == NULL)
	{
		printf("File Not Writable hence returning\n");
		return;
	}

	
	aborts = paborts + uaborts;
	fprintf(fp_apis,"###########################################\n");
	fprintf(fp_apis,"Time %s \n SRI-SM-REQUEST [%lu] SRI-SM-CONFIRM Confirms [%lu] \n  USSR-REQUEST [%lu] USSR-CONFIRM Confirms [%lu] \n ABORTS [%d] Local Cancellation [%lu] \n",ctime(&tval),srism_request_counters,srism_confirm_counters,ussd_request_counters,ussd_confirm_counters,aborts,local_cacel_counters);

	fprintf(fp_apis,"###########################################\n");

	fclose(fp_apis);

		
}

void print_sri_sm_lu_report()
{
	FILE *fp_apis =NULL;
	time_t tval;
	char filename[100] = "./REPORTS";
	time(&tval);
	signal (SIGUSR1, sigusr1_hdlr);
	signal (SIGUSR2, sigusr2_hdlr);

	fflush(stdout); fflush(stdin);

	fp_apis = fopen(filename,"a+");
	if(fp_apis == NULL)
	{
		printf("File Not Writable hence returning\n");
		return;
	}

	
	aborts = paborts + uaborts;
	fprintf(fp_apis,"###########################################\n");
	fprintf(fp_apis,"Time %s \n SRI-REQUEST [%lu] SRI-CONFIRM Confirms [%lu] \n  SRI-SM-REQUEST [%lu] SRI-SM-CONFIRM Confirms [%lu] \n LU-REQUEST [%lu] LU-CONFIRM Confirms [%lu] \n ABORTS [%d] Local Cancellation [%lu] \n",ctime(&tval),sri_request_counters,sri_confirm_counters,srism_request_counters,srism_confirm_counters,lu_request_counters,lu_confirm_counters,aborts,local_cacel_counters);

	fprintf(fp_apis,"###########################################\n");

	fclose(fp_apis);

		
}





void send_delimiter_request(int corr_id)
{
   int error =0; 
   map_api_struct_t  *send_api = NULL;
   map_delimiter_request_t *map_delimiter_req = NULL;


   send_api = app_mem_get(sizeof(map_api_struct_t));
   map_memzero(send_api,sizeof(map_api_struct_t));


   map_delimiter_req = app_mem_get(sizeof(map_delimiter_request_t));
   map_memzero(map_delimiter_req,sizeof(map_delimiter_request_t));

   map_delimiter_req->is_dialog_id = 0;
   map_delimiter_req->is_corr_id = 1;
   map_delimiter_req->corr_id = corr_id;



   fill_api_header(&(send_api->header), vlr_user_id);
   send_api->header.api_id = MAP_DELIMITER_REQUEST;
   send_api->header.spare1 = g_sap;
   send_api->header.ver = 3;
   send_api->header.len = sizeof(map_delimiter_request_t);
   send_api->p_data = map_delimiter_req;
   app_map_send_to_app_map((unsigned char *)send_api, &error);

}

/* Fills the originating address*/
void fillOrignatingAdd(map_open_request_t *open,int ssn)
{
		open->is_orig_add =1;
		open->orig_add.routing_ind = ROUTE_ON_SSN; /* 1 for Route on SSN and 0 for GT*/
		open->orig_add.is_spc = 1;
		//open->orig_add.is_spc = 0;
		open->orig_add.spc = g_spc;
		open->orig_add.is_ssn = 1;
		open->orig_add.ssn = ssn;
		open->orig_add.is_gt = 0;
		//open->orig_add.is_gt = 1;
		open->orig_add.global_title_ind = MAP_GT_WITH_TT_NP_ES_NAI; /*Section 3.4.1 possible values = MAP_NO_GT,MAP_GT_WIT
																	  H_NAI,MAP_GT_WITH_TT,MAP_GT_WITH_TT_NP_ES,MAP_GT_WITH_TT_NP_ES_NAI,MAP_GT_RESERVED */
		open->orig_add.gt.type4.translation_type = 0;
		open->orig_add.gt.type4.numbering_plan = 1;
		open->orig_add.gt.type4.encoding_scheme = 2;
		open->orig_add.gt.type4.nature_of_addr_ind = 4;
		open->orig_add.gt.type4.num_gt_addr_info_octets = 6;
		//open->orig_add.gt.type4.num_gt_addr_info_octets = 256;
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

	open_req = (map_open_request_t *)app_mem_get(sizeof(map_open_request_t));
	map_memzero(open_req,sizeof(map_open_request_t));

	send_open_req_api = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
	map_memzero(send_open_req_api,sizeof(map_api_struct_t));


	if(!strncmp(api_name,"MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST")))
	{
		luser_id = msc_user_id;
		lssn = SMSC_SSN;
		dest_ssn = SMSC_SSN;
		options = MAP_AC_SHORT_MSG_RELAY;	
		version = 3;
	}
    if(!strncmp(api_name,"MAP_V2_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_V2_FORWARD_SHORT_MESSAGE_REQUEST")))
	{
		luser_id = msc_user_id;
		lssn = SMSC_SSN;
		dest_ssn = SMSC_SSN;
		options = MAP_AC_SHORT_MSG_RELAY;	
		version = 2;
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
		luser_id = hlr_user_id;
		lssn = HLR_SSN;
		dest_ssn = MSC_SSN;
		options = MAP_AC_SHORT_MSG_ALERT;
		version = 1;

	}
    else
    if(!strncmp(api_name,"MAP_V2_PROCESS_UNSTRUCTURED_SS_DATA_REQUEST",strlen("MAP_V2_PROCESS_UNSTRUCTURED_SS_DATA_REQUEST")))
    {

    	luser_id = vlr_user_id;
		lssn = VLR_SSN;
		dest_ssn = VLR_SSN;
		options = MAP_AC_NETWORK_UNSTRUCTURED_SS;	
		version = 2;

    }
    else
    if(!strncmp(api_name,"MAP_V1_PROCESS_UNSTRUCTURED_SS_DATA_REQUEST",strlen("MAP_V1_PROCESS_UNSTRUCTURED_SS_DATA_REQUEST")))
    {

    	luser_id = hlr_user_id;
		lssn = HLR_SSN;
		dest_ssn = HLR_SSN;
		options = MAP_AC_NETWORK_FUNCTIONAL_SS ;	
		version = 1;

    }

    else if(!strncmp(api_name,"MAP_V2_UNSTRUCTURED_SS_REQUEST_REQUEST",strlen("MAP_V2_UNSTRUCTURED_SS_REQUEST_REQUEST")))
    {

    	luser_id = vlr_user_id;
		lssn = VLR_SSN;
		dest_ssn = VLR_SSN;
		options = MAP_AC_NETWORK_UNSTRUCTURED_SS;	
		version = 2;

    }
    else if(!strncmp(api_name,"MAP_V2_UNSTRUCTURED_SS_NOTIFY_REQUEST",strlen("MAP_V2_UNSTRUCTURED_SS_NOTIFY_REQUEST")))
    {

    	luser_id = vlr_user_id;
		lssn = VLR_SSN;
		dest_ssn = VLR_SSN;
		options = MAP_AC_NETWORK_UNSTRUCTURED_SS;	
		version = 2;

    }
    else if(!strncmp(api_name,"MAP_V2_MT_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_V2_MT_FORWARD_SHORT_MESSAGE_REQUEST")))
    {
    	luser_id = msc_user_id;
		lssn = SMSC_SSN;
		dest_ssn = SMSC_SSN;
		options = MAP_AC_SHORT_MSG_RELAY;
		version = 2;

    }
    else if(!strncmp(api_name,"MAP_V2_CHECK_IMEI_REQUEST",strlen("MAP_V2_CHECK_IMEI_REQUEST")))
    {
    	luser_id = msc_user_id;
		lssn = SMSC_SSN;
		dest_ssn = EIR_SSN;
		options = MAP_AC_EQUIPMENT_MNGT;
		version = 2;

    }
    else if(!strncmp(api_name,"MAP_V1_CHECK_IMEI_REQUEST",strlen("MAP_V1_CHECK_IMEI_REQUEST")))
    {
    	luser_id = msc_user_id;
		lssn = SMSC_SSN;
		dest_ssn = EIR_SSN;
		options = MAP_AC_EQUIPMENT_MNGT;
		version = 1;

    }
    else if(!strncmp(api_name,"MAP_CHECK_IMEI_REQUEST",strlen("MAP_CHECK_IMEI_REQUEST")))
    {
    	luser_id = msc_user_id;
		lssn = MSC_SSN;
		dest_ssn = EIR_SSN;
		options = MAP_AC_EQUIPMENT_MNGT;
		version = 3;

    }

    else if(!strncmp(api_name,"MAP_V2_ALERT_SERVICE_CENTRE_REQUEST",strlen("MAP_V2_ALERT_SERVICE_CENTRE_REQUEST")))
    {
    	luser_id = hlr_user_id;
		lssn = HLR_SSN;
		dest_ssn = MSC_SSN;
		options = MAP_AC_SHORT_MSG_ALERT;
		version = 2;

    }
    else if(!strncmp(api_name,"MAP_V2_INFORM_SERVICE_CENTRE_REQUEST",strlen("MAP_V2_INFORM_SERVICE_CENTRE_REQUEST")))
    {
    	luser_id = msc_user_id;
		lssn = MSC_SSN;
		dest_ssn = HLR_SSN;
		options = MAP_AC_SHORT_MSG_GATEWAY;
		version = 2;

    }
    else if(!strncmp(api_name,"MAP_SEND_ROUTING_INFORMATION_REQUEST",strlen("MAP_SEND_ROUTING_INFORMATION_REQUEST")))
    {
    	luser_id = msc_user_id;
		lssn = MSC_SSN;
		dest_ssn = HLR_SSN;
		options = MAP_AC_LOC_INFO_RETRIEVAL;
		version = 3;

    }
    else if(!strncmp(api_name,"MAP_V1_SEND_ROUTING_INFORMATION_REQUEST",strlen("MAP_V1_SEND_ROUTING_INFORMATION_REQUEST")))
    {
    	luser_id = msc_user_id;
		lssn = MSC_SSN;
		dest_ssn = HLR_SSN;
		options = MAP_AC_LOC_INFO_RETRIEVAL;
		version = 1;

    }
    else if(!strncmp(api_name,"MAP_V2_SEND_ROUTING_INFORMATION_REQUEST",strlen("MAP_V2_SEND_ROUTING_INFORMATION_REQUEST")))
    {
    	luser_id = msc_user_id;
		lssn = MSC_SSN;
		dest_ssn = HLR_SSN;
		options = MAP_AC_LOC_INFO_RETRIEVAL;
		version = 2;

    }
    else if(!strncmp(api_name,"MAP_ROUTING_INFO_FOR_SM",strlen("MAP_ROUTING_INFO_FOR_SM")))
    {
    	luser_id = msc_user_id;
		lssn = MSC_SSN;
		dest_ssn = HLR_SSN;
		options = MAP_AC_SHORT_MSG_GATEWAY;
		version = 3;

    }
    else if(!strncmp(api_name,"MAP_V2_ROUTING_INFO_FOR_SM",strlen("MAP_V2_ROUTING_INFO_FOR_SM")))
    {
    	luser_id = msc_user_id;
		lssn = MSC_SSN;
		dest_ssn = HLR_SSN;
		options = MAP_AC_SHORT_MSG_GATEWAY;
		version = 2;

    }
    else if(!strncmp(api_name,"MAP_V1_ROUTING_INFO_FOR_SM",strlen("MAP_V1_ROUTING_INFO_FOR_SM")))
    {
    	luser_id = msc_user_id;
		lssn = MSC_SSN;
		dest_ssn = HLR_SSN;
		options = MAP_AC_SHORT_MSG_GATEWAY;
		version = 1;

    }

    else if(!strncmp(api_name,"MAP_REPORT_SM_DELIVERY_STATUS_REQUEST",strlen("MAP_REPORT_SM_DELIVERY_STATUS_REQUEST")))
    {
    	luser_id = msc_user_id;
		lssn = MSC_SSN;
		dest_ssn = HLR_SSN;
		options = MAP_AC_SHORT_MSG_GATEWAY;
		version = 3;

    }
    else if(!strncmp(api_name,"MAP_V2_REPORT_SM_DELIVERY_STATUS_REQUEST",strlen("MAP_V2_REPORT_SM_DELIVERY_STATUS_REQUEST")))
    {
    	luser_id = msc_user_id;
		lssn = MSC_SSN;
		dest_ssn = HLR_SSN;
		options = MAP_AC_SHORT_MSG_GATEWAY;
		version = 2;

    }
    else if(!strncmp(api_name,"MAP_INFORM_SERVICE_CENTRE_REQUEST",strlen("MAP_INFORM_SERVICE_CENTRE_REQUEST")))
    {
    	luser_id = msc_user_id;
		lssn = MSC_SSN;
		dest_ssn = HLR_SSN;
		options = MAP_AC_SHORT_MSG_GATEWAY;
		version = 3;

    }
    else if(!strncmp(api_name,"MAP_PROVIDE_ROAMING_NUMBER_REQUEST",strlen("MAP_PROVIDE_ROAMING_NUMBER_REQUEST")))
    {
    	luser_id = hlr_user_id;
		lssn = HLR_SSN;
		dest_ssn = VLR_SSN;
		options = MAP_AC_ROAMING_NB_ENQUIRY;
		version = 3;

    }
    else
    if(!strncmp(api_name,"MAP_V1_PROVIDE_ROAMING_NUMBER_REQUEST",strlen("MAP_V1_PROVIDE_ROAMING_NUMBER_REQUEST")))
    {
    	luser_id = hlr_user_id;
		lssn = HLR_SSN;
		dest_ssn = VLR_SSN;
		options = MAP_AC_ROAMING_NB_ENQUIRY;
		version = 1;

    }
    else if(!strncmp(api_name,"MAP_V2_PROVIDE_ROAMING_NUMBER_REQUEST",strlen("MAP_V2_PROVIDE_ROAMING_NUMBER_REQUEST")))
    {
    	luser_id = hlr_user_id;
		lssn = HLR_SSN;
		dest_ssn = VLR_SSN;
		options = MAP_AC_ROAMING_NB_ENQUIRY;
		version = 2;

    }
    else if(!strncmp(api_name,"MAP_SET_REPORTING_STATE_REQUEST",strlen("MAP_SET_REPORTING_STATE_REQUEST")))
    {
    	luser_id = hlr_user_id;
		lssn = HLR_SSN;
		dest_ssn = VLR_SSN;
		options = MAP_AC_REPORTING;
		version = 3;

    }
    else if(!strncmp(api_name,"MAP_REMOTE_USER_FREE_REQUEST",strlen("MAP_REMOTE_USER_FREE_REQUEST")))
    {
    	luser_id = hlr_user_id;
		lssn = HLR_SSN;
		dest_ssn = VLR_SSN;
		options = MAP_AC_REPORTING;
		version = 3;

    }

    else if(!strncmp(api_name,"MAP_STATUS_REPORT_REQUEST",strlen("MAP_STATUS_REPORT_REQUEST")))
    {
    	luser_id = vlr_user_id;
		lssn = VLR_SSN;
		dest_ssn = HLR_SSN;
		options = MAP_AC_REPORTING;
		version = 3;

    }
    else if(!strncmp(api_name,"MAP_IST_ALERT_REQUEST",strlen("MAP_IST_ALERT_REQUEST")))
    {
    	luser_id = msc_user_id;
		lssn = MSC_SSN;
		dest_ssn = HLR_SSN;
		options = MAP_AC_ALERTING;
		version = 3;

    }
    else if(!strncmp(api_name,"MAP_IST_COMMAND_REQUEST",strlen("MAP_IST_COMMAND_REQUEST")))
    {
    	luser_id = hlr_user_id;
		lssn = HLR_SSN;
		dest_ssn = MSC_SSN;
		options = MAP_AC_SERVICE_TERMINATION;
		version = 3;

    }
    else if(!strncmp(api_name,"MAP_RESTORE_DATA_REQUEST",strlen("MAP_RESTORE_DATA_REQUEST")))
    {
    	luser_id = vlr_user_id;
		lssn = VLR_SSN;
		dest_ssn = HLR_SSN;
		options =MAP_AC_NETWORK_LOC_UP ;
		version = 3;

    }
    else if(!strncmp(api_name,"MAP_INSERT_SUBSCRIBER_DATA_REQUEST",strlen("MAP_INSERT_SUBSCRIBER_DATA_REQUEST")))
    {
    	luser_id = hlr_user_id;
		lssn = HLR_SSN;
		dest_ssn = VLR_SSN;
		options =MAP_AC_SUBSCRIBER_DATA_MNGT;
		version = 3;

    }
     else if(!strncmp(api_name,"MAP_LOCATION_UPDATE_REQUEST",strlen("MAP_LOCATION_UPDATE_REQUEST")))
    {
    	luser_id = vlr_user_id;
		lssn = VLR_SSN;
		dest_ssn = HLR_SSN;
		options =MAP_AC_NETWORK_LOC_UP;
		version = 3;

    }
     else if(!strncmp(api_name,"MAP_V1_LOCATION_UPDATE_REQUEST",strlen("MAP_V1_LOCATION_UPDATE_REQUEST")))
    {
    	luser_id = vlr_user_id;
		lssn = VLR_SSN;
		dest_ssn = HLR_SSN;
		options =MAP_AC_NETWORK_LOC_UP;
		version = 1;

    }
    else if(!strncmp(api_name,"MAP_PURGE_MS_REQUEST",strlen("MAP_PURGE_MS_REQUEST")))
    {
    	luser_id = vlr_user_id;
		lssn = VLR_SSN;
		dest_ssn = HLR_SSN;
		options = MAP_AC_MS_PURGING;
		version = 2;

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
	//send_api->header.spare1 = 1;
	send_api->header.spare1 = g_sap;
	send_api->p_data = close_req;
	app_map_send_to_app_map((unsigned char *)send_api, &error);
		

}

void send_messages(char *api_name,int num_of_messages,int flag_dialogue,int no_mess_in_burst,int delay)
{
	
	unsigned char line_val[MAX_LINE_BYTE];
	char filename[100] = {'\0',},bitmap_array[20] = {'\0',};
	char *token_length = NULL,*token_val = NULL,*token_generic,*token_ext = NULL,*token_val_temp = NULL;
	FILE *fp_apis =NULL;
	int message_counter = 0,index = 0,counter = 0,buffer_val = 0,error =0,cycle_counter = 0,ext_counter =0,num_cycles=0;
    struct timeval tout;
	
	/* Code changes for Testing */
	static int corr_id=0,tmp_var=0;

	memset(bitmap_array,'N',20);


    /* Calculating Number of Burst */
    num_cycles = num_of_messages/no_mess_in_burst;


	//for(cycle_counter = 0 ; cycle_counter < num_cycles ; cycle_counter++)
	{

		if((!strncmp(api_name,"MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST")))) 
		{
			index = 0;


            if(send_api_mofsm == NULL)
            {
                send_api_mofsm = app_mem_get(sizeof(map_api_struct_t));
                map_memzero(send_api_mofsm,sizeof(map_api_struct_t));

                sprintf(filename,"../buffers/%s",api_name);
                fp_apis = fopen(filename,"r");
            }

			//printf("API Name Matches MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST \n");

            if(p_v1_fsm_req == NULL)
            {
                fill_api_header(&(send_api_mofsm->header), msc_user_id);
                send_api_mofsm->header.api_id = MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST;
                send_api_mofsm->header.spare1 = g_sap;
                send_api_mofsm->header.ver = 3;
                send_api_mofsm->header.len = sizeof(map_mo_forward_sm_request_t);




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

                            token_val_temp = (char *)malloc(strlen(token_val));

                            strcpy(token_val_temp,token_val);

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
                                    //printf("Token _Generic Index 0 Choice [%s]\n",token_generic);
                                    //p_v1_fsm_req->arg.sm_rp_da.choice = atoi(token_generic);
                                    //buffer_val = hstoi(token_generic);
                                    p_v1_fsm_req->arg.sm_rp_da.choice = atoi(token_generic);
                                }
                                else if(index == 1)
                                {
                                    if(p_v1_fsm_req->arg.sm_rp_da.choice == 1)
                                    {

                                        //printf("Token _Generic Index 1 Length [%s]\n",token_length);
                                        //p_v1_fsm_req->arg.sm_rp_da.u.imsi.length = atoi(token_length);
                                        //p_v1_fsm_req->arg.sm_rp_da.u.imsi.value[counter] =  atoi(token_generic);
                                        //buffer_val = hstoi(token_length);
                                        p_v1_fsm_req->arg.sm_rp_da.u.imsi.length = atoi(token_length);
                                        buffer_val = hstoi(token_generic);
                                        p_v1_fsm_req->arg.sm_rp_da.u.imsi.value[counter] = buffer_val;

                                        //printf("Token _Generic Index 1 value [%s]\n",token_generic);
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
                                else if(index == 4)
                                {

                                    if(!strncmp(api_name,"MAP_V1_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_V1_FORWARD_SHORT_MESSAGE_REQUEST")))
                                    {
                                        p_v1_fsm_req->arg.sm_rp_ui.length = atoi(token_length);
                                        buffer_val = hstoi(token_generic);
                                        p_v1_fsm_req->arg.sm_rp_ui.value[counter] = buffer_val;
                                    }
                                    else if(!strncmp(api_name,"MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST")))
                                    {
                                        p_v1_fsm_req->arg.sm_rp_ui.length = atoi(token_generic);
                                    }
                                }
                                else if(index == 5)
                                {

                                    strncpy(p_v1_fsm_req->arg.sm_rp_ui.value,token_generic,p_v1_fsm_req->arg.sm_rp_ui.length);


                                }
                                else if(index == 6)
                                {

                                    p_v1_fsm_req->arg.is_extension = atoi(token_generic);
                                }
                                else if(index == 7)
                                {
                                    p_v1_fsm_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

                                    for(ext_counter = 0; ext_counter <p_v1_fsm_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
                                    {
                                        int finished_flag = 0;
                                        if(ext_counter ==0)	
                                        {
                                            token_ext = strtok(token_val_temp,":");
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
                                                    p_v1_fsm_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


                                                }
                                                else
                                                {
                                                    p_v1_fsm_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
                                                }
                                            }

                                            finished_flag++;

                                        }


                                    }

                                }
                                else if(index == 8)
                                {

                                    for(ext_counter = 0; ext_counter <p_v1_fsm_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
                                    {
                                        int finished_flag = 0;
                                        if(ext_counter ==0)	
                                        {
                                            token_ext = strtok(token_val_temp,":");
                                        }		
                                        else{
                                            token_ext = strtok(NULL,":");
                                        }


                                        if(!strncmp(token_ext,"NULL",strlen("NULL")))
                                            p_v1_fsm_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
                                        else
                                        {	
                                            p_v1_fsm_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
                                                        p_v1_fsm_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


                                                    }
                                                    else
                                                    {
                                                        p_v1_fsm_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
                                                    }
                                                }

                                                finished_flag++;

                                            }
                                        }

                                    }
                                }
                                else if(index == 9)
                                {
                                    p_v1_fsm_req->arg.is_imsi =  atoi(token_generic);
                                }
                                else if(index == 10)
                                {

                                    p_v1_fsm_req->arg.imsi.length = hstoi(token_length);
                                    p_v1_fsm_req->arg.imsi.value[counter] = hstoi(token_generic);
                                }
                            }
                            index++;
                        }
                        else{

                            //Fill 0 Values

                        }

                    }

                }

            }

	
            expiry_time.tv_sec = 0;	
            expiry_time.tv_usec = 0;
            for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
            {

                #if 0
                if (gettimeofday(&current_time,NULL) == 0)
                {
                    if (current_time.tv_sec > expiry_time.tv_sec || ( current_time.tv_sec == expiry_time.tv_sec && current_time.tv_usec >= expiry_time.tv_usec))
                    {

                        expiry_time.tv_sec =    current_time.tv_sec + (current_time.tv_usec + delay)/1000000;
                        expiry_time.tv_usec =   (current_time.tv_usec + delay)%1000000;


                #endif
                        for(message_counter = 0; message_counter < no_mess_in_burst;message_counter++ )
                        {


                            if(seq_control == 15)
                                seq_control = 1;
                            else
                                seq_control++;

                            send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
                            //map_memzero(send_api_temp,sizeof(map_api_struct_t));

                            p_v1_fsm_req_temp = (map_mo_forward_sm_request_t*)app_mem_get(sizeof(map_mo_forward_sm_request_t)); 
                            //map_memzero(p_v1_fsm_req_temp, sizeof(map_mo_forward_sm_request_t));




                            /*
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

                             */

                            corr_id = app_map_get_new_correlation_id();
                            p_v1_fsm_req->header.corr_id =  corr_id;
                            send_open_req(api_name ,corr_id);
                            //}


                            memcpy(p_v1_fsm_req_temp,p_v1_fsm_req,sizeof(map_mo_forward_sm_request_t));
                            memcpy(send_api_temp,send_api_mofsm,sizeof(map_api_struct_t));

                            p_v1_fsm_req_temp->header.invoke_id = message_counter + 1;

                            //if(message_counter == (num_of_messages -1))
                            p_v1_fsm_req_temp->header.last_component = 1;

                            send_api_temp->p_data = p_v1_fsm_req_temp;
                            app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

                            mo_request_counters++;

                            if((mo_request_counters % 50000) == 0)
                                print_mt_mo_report();

                        }


                        usleep(delay);
                        //sleep(1);


                #if 0
                    }

                    else
                    {
                        //	printf("I m sleeping for [%d]",(((expiry_time.tv_sec*1000000) + expiry_time.tv_usec)  - (( current_time.tv_sec*1000000) + current_time.tv_usec))/1001);
                        usleep(((expiry_time.tv_sec*1000000) + expiry_time.tv_usec)  - (( current_time.tv_sec*1000000) + current_time.tv_usec));
                    }

                
                }

                #endif


            }





            #if 0
			app_mem_free(p_v1_fsm_req);
			app_mem_free(send_api_mofsm);

            #endif


		}
		else if((!strncmp(api_name,"MAP_V1_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_V1_FORWARD_SHORT_MESSAGE_REQUEST")))) 
		{
			index = 0;

			map_v1_forward_short_message_request_t	*p_v1_fsm_req = NULL,*p_v1_fsm_req_temp = NULL;

			printf("API Name Matches MAP_V1_FORWARD_SHORT_MESSAGE_REQUEST \n");
			fill_api_header(&(send_api->header), msc_user_id);
			send_api->header.api_id = MAP_FORWARD_SHORT_MESSAGE_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 1;
			send_api->header.len = sizeof(map_v1_forward_short_message_request_t);


			p_v1_fsm_req = (map_v1_forward_short_message_request_t*)app_mem_get(sizeof(map_v1_forward_short_message_request_t)); 
			map_memzero(p_v1_fsm_req, sizeof(map_v1_forward_short_message_request_t));


			printf(" MAP_V1_FORWARD_SHORT_MESSAGE_REQUEST \n");
			fill_header(&p_v1_fsm_req->header,corr_id); 


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			strncpy(bitmap_array,line_val,20);




			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_V1_FORWARD_SHORT_MESSAGE_REQUEST is successfully Parsed\n\n");
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
										p_v1_fsm_req->arg.sm_rp_da.u.l_ms_id.length = atoi(token_length);
										buffer_val = hstoi(token_generic);
										p_v1_fsm_req->arg.sm_rp_da.u.l_ms_id.value[counter] = buffer_val;

								}
								else if(p_v1_fsm_req->arg.sm_rp_da.choice == 3)
								{
									p_v1_fsm_req->arg.sm_rp_da.u.roaming_number.length = atoi(token_length);
									buffer_val = hstoi(token_generic);
									p_v1_fsm_req->arg.sm_rp_da.u.roaming_number.value[counter] = buffer_val;

								}
								else if(p_v1_fsm_req->arg.sm_rp_da.choice == 4)
								{
									p_v1_fsm_req->arg.sm_rp_da.u.service_centre_address.length = atoi(token_length);
									buffer_val = hstoi(token_generic);
									p_v1_fsm_req->arg.sm_rp_da.u.service_centre_address.value[counter] = buffer_val;


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

									p_v1_fsm_req->arg.sm_rp_oa.u.ms_isdn.length = atoi(token_length);
									buffer_val = hstoi(token_generic);
									p_v1_fsm_req->arg.sm_rp_oa.u.ms_isdn.value[counter] = buffer_val;


								}
								else if(p_v1_fsm_req->arg.sm_rp_oa.choice == 2)
								{
									//p_v1_fsm_req->arg.sm_rp_oa.u.service_centre_address.length=atoi(token_length);
									//p_v1_fsm_req->arg.sm_rp_oa.u.service_centre_address.value[counter]=atoi(token_generic);
									//buffer_val = hstoi(token_length);
									p_v1_fsm_req->arg.sm_rp_oa.u.service_centre_address.length = atoi(token_length);
									buffer_val = hstoi(token_generic);
									p_v1_fsm_req->arg.sm_rp_oa.u.service_centre_address.value[counter] = buffer_val;

								}
								else{
									printf("Not a valid Choice hence exiting\n");

									exit(1);
								}

							}
							else if(index == 4)
							{

								p_v1_fsm_req->arg.sm_rp_ui.length = atoi(token_length);
								buffer_val = hstoi(token_generic);
								p_v1_fsm_req->arg.sm_rp_ui.value[counter] = buffer_val;
							}
						}
						index++;
					}
					else{

						//Fill 0 Values

					}

				}

			}

	

			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v1_fsm_req_temp = (map_v1_forward_short_message_request_t*)app_mem_get(sizeof(map_v1_forward_short_message_request_t)); 
					map_memzero(p_v1_fsm_req_temp, sizeof(map_v1_forward_short_message_request_t));



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


					memcpy(p_v1_fsm_req_temp,p_v1_fsm_req,sizeof(map_v1_forward_short_message_request_t));
					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v1_fsm_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
						p_v1_fsm_req_temp->header.last_component = 1;
					
					send_api_temp->p_data = p_v1_fsm_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

				//sleep(1);

			}

			app_mem_free(p_v1_fsm_req);
			app_mem_free(send_api);


		}

         else if((!strncmp(api_name,"MAP_V2_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_V2_FORWARD_SHORT_MESSAGE_REQUEST")))) 
		{
			index = 0;

			map_v2_forward_sm_request_t	*p_v1_fsm_req = NULL,*p_v1_fsm_req_temp = NULL;

			printf("API Name Matches MAP_V2_FORWARD_SHORT_MESSAGE_REQUEST \n");
			fill_api_header(&(send_api->header), msc_user_id);
			send_api->header.api_id = MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST ;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 2;
			send_api->header.len = sizeof(map_v2_forward_sm_request_t);


			p_v1_fsm_req = (map_v2_forward_sm_request_t *)app_mem_get(sizeof(map_v2_forward_sm_request_t)); 
			map_memzero(p_v1_fsm_req, sizeof(map_v2_forward_sm_request_t));


			printf(" MAP_V2_FORWARD_SHORT_MESSAGE_REQUEST \n");
			fill_header(&p_v1_fsm_req->header,corr_id); 


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			strncpy(bitmap_array,line_val,20);




			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_V2_FORWARD_SHORT_MESSAGE_REQUEST is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)    
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
										p_v1_fsm_req->arg.sm_rp_da.u.imsi.length = atoi(token_length);
										buffer_val = hstoi(token_generic);
										p_v1_fsm_req->arg.sm_rp_da.u.imsi.value[counter] = buffer_val;

								}
								else if(p_v1_fsm_req->arg.sm_rp_da.choice == 3)
								{
									p_v1_fsm_req->arg.sm_rp_da.u.roaming_number.length = atoi(token_length);
									buffer_val = hstoi(token_generic);
									p_v1_fsm_req->arg.sm_rp_da.u.roaming_number.value[counter] = buffer_val;

								}
								else if(p_v1_fsm_req->arg.sm_rp_da.choice == 4)
								{
									p_v1_fsm_req->arg.sm_rp_da.u.service_centre_address_da.length = atoi(token_length);
									buffer_val = hstoi(token_generic);
									p_v1_fsm_req->arg.sm_rp_da.u.service_centre_address_da.value[counter] = buffer_val;


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

									p_v1_fsm_req->arg.sm_rp_oa.u.msisdn.length = atoi(token_length);
									buffer_val = hstoi(token_generic);
									p_v1_fsm_req->arg.sm_rp_oa.u.msisdn.value[counter] = buffer_val;


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
							else if(index == 4)
							{

								p_v1_fsm_req->arg.sm_rp_ui.length = atoi(token_length);
								buffer_val = hstoi(token_generic);
								p_v1_fsm_req->arg.sm_rp_ui.value[counter] = buffer_val;
							}
                            else if(index == 5)
                            {
                                p_v1_fsm_req->arg.is_more_messages_to_send=atoi(token_generic);

                            }

                                

						}
						index++;
					}
					else{

						//Fill 0 Values

					}

				}

			}

	

			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v1_fsm_req_temp = (map_v2_forward_sm_request_t*)app_mem_get(sizeof(map_v2_forward_sm_request_t)); 
					map_memzero(p_v1_fsm_req_temp, sizeof(map_v2_forward_sm_request_t));



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


					memcpy(p_v1_fsm_req_temp,p_v1_fsm_req,sizeof(map_v2_forward_sm_request_t));
					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v1_fsm_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
						p_v1_fsm_req_temp->header.last_component = 1;
					
					send_api_temp->p_data = p_v1_fsm_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

				//sleep(1);

			}

			app_mem_free(p_v1_fsm_req);
			app_mem_free(send_api);


		}
        else if((!strncmp(api_name,"MAP_V2_MT_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_V2_MT_FORWARD_SHORT_MESSAGE_REQUEST")))) 
		{
			index = 0;

			map_v2_forward_sm_request_t	*p_v1_fsm_req = NULL,*p_v1_fsm_req_temp = NULL;

			printf("API Name Matches MAP_V2_MT_FORWARD_SHORT_MESSAGE_REQUEST \n");
			fill_api_header(&(send_api->header), msc_user_id);
			send_api->header.api_id = MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST ;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 2;
			send_api->header.len = sizeof(map_v2_forward_sm_request_t);


			p_v1_fsm_req = (map_v2_forward_sm_request_t *)app_mem_get(sizeof(map_v2_forward_sm_request_t)); 
			map_memzero(p_v1_fsm_req, sizeof(map_v2_forward_sm_request_t));


			printf(" MAP_V2_MT_FORWARD_SHORT_MESSAGE_REQUEST \n");
			fill_header(&p_v1_fsm_req->header,corr_id); 


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			strncpy(bitmap_array,line_val,20);




			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_V2_MT_FORWARD_SHORT_MESSAGE_REQUEST is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)    
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
										p_v1_fsm_req->arg.sm_rp_da.u.imsi.length = atoi(token_length);
										buffer_val = hstoi(token_generic);
										p_v1_fsm_req->arg.sm_rp_da.u.imsi.value[counter] = buffer_val;

								}
								else if(p_v1_fsm_req->arg.sm_rp_da.choice == 3)
								{
									p_v1_fsm_req->arg.sm_rp_da.u.roaming_number.length = atoi(token_length);
									buffer_val = hstoi(token_generic);
									p_v1_fsm_req->arg.sm_rp_da.u.roaming_number.value[counter] = buffer_val;

								}
								else if(p_v1_fsm_req->arg.sm_rp_da.choice == 4)
								{
									p_v1_fsm_req->arg.sm_rp_da.u.service_centre_address_da.length = atoi(token_length);
									buffer_val = hstoi(token_generic);
									p_v1_fsm_req->arg.sm_rp_da.u.service_centre_address_da.value[counter] = buffer_val;


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

									p_v1_fsm_req->arg.sm_rp_oa.u.msisdn.length = atoi(token_length);
									buffer_val = hstoi(token_generic);
									p_v1_fsm_req->arg.sm_rp_oa.u.msisdn.value[counter] = buffer_val;


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
							else if(index == 4)
							{

								p_v1_fsm_req->arg.sm_rp_ui.length = atoi(token_length);
								buffer_val = hstoi(token_generic);
								p_v1_fsm_req->arg.sm_rp_ui.value[counter] = buffer_val;
							}
                            else if(index == 5)
                            {
                                p_v1_fsm_req->arg.is_more_messages_to_send=atoi(token_generic);

                            }

                                

						}
						index++;
					}
					else{

						//Fill 0 Values

					}

				}

			}

	

			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v1_fsm_req_temp = (map_v2_forward_sm_request_t*)app_mem_get(sizeof(map_v2_forward_sm_request_t)); 
					map_memzero(p_v1_fsm_req_temp, sizeof(map_v2_forward_sm_request_t));



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


					memcpy(p_v1_fsm_req_temp,p_v1_fsm_req,sizeof(map_v2_forward_sm_request_t));
					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v1_fsm_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
						p_v1_fsm_req_temp->header.last_component = 1;
					
					send_api_temp->p_data = p_v1_fsm_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

				//sleep(1);

			}

			app_mem_free(p_v1_fsm_req);
			app_mem_free(send_api);


		}


		else if(!strncmp(api_name,"MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST")))
		{

			index = 0;  //Making index to 0 for next processing



            if(send_api_mtfsm == NULL)
            {
                send_api_mtfsm = app_mem_get(sizeof(map_api_struct_t));
                map_memzero(send_api_mtfsm,sizeof(map_api_struct_t));

                sprintf(filename,"../buffers/%s",api_name);
                fp_apis = fopen(filename,"r");


            }

            if(p_mt_fsm_req == NULL)
            {
                p_mt_fsm_req = (map_mt_forward_sm_request_t*)app_mem_get(sizeof(map_mt_forward_sm_request_t));
                map_memzero(p_mt_fsm_req,sizeof(map_mt_forward_sm_request_t));

                printf("API Name Matches MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST \n");
                fill_api_header(&(send_api_mtfsm->header), user_id);
                send_api_mtfsm->header.api_id = MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST;
                send_api_mtfsm->header.spare1 = g_sap;
                send_api_mtfsm->header.ver = 3;
                send_api_mtfsm->header.len = sizeof(map_mt_forward_sm_request_t);

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


                            token_val_temp = (char *)malloc(strlen(token_val));


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
                                        //buffer_val = hstoi(token_generic);
                                        p_mt_fsm_req->arg.sm_rp_da.u.imsi.value[counter] = hstoi(token_generic) ;

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

                                        //buffer_val = hstoi(token_generic);
                                        p_mt_fsm_req->arg.sm_rp_oa.u.service_centre_address_oa.value[counter] = hstoi(token_generic);


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
                                    //p_mt_fsm_req->arg.sm_rp_ui.length = atoi(token_length);
                                    //p_mt_fsm_req->arg.sm_rp_ui.value[counter] = hstoi(token_generic);

                                }
                                else if(index == 5)
                                {
                                    p_mt_fsm_req->arg.is_more_messages_to_send = atoi(token_generic);
                                }
                                else if(index == 6 )
                                {

                                    p_mt_fsm_req->arg.more_messages_to_send = atoi(token_generic);
                                }
                                else if(index == 7)
                                {

                                    p_mt_fsm_req->arg.is_extension = atoi(token_generic);
                                }
                                else if(index == 8)
                                {
                                    p_mt_fsm_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

                                    for(ext_counter = 0; ext_counter <p_mt_fsm_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
                                    {
                                        int finished_flag = 0;
                                        if(ext_counter ==0)	
                                        {
                                            token_ext = strtok(token_val_temp,":");
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
                                                    p_mt_fsm_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


                                                }
                                                else
                                                {
                                                    p_mt_fsm_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
                                                }
                                            }

                                            finished_flag++;

                                        }


                                    }

                                }
                                else if(index == 9)
                                {

                                    for(ext_counter = 0; ext_counter <p_mt_fsm_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
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
                                            p_mt_fsm_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
                                        else
                                        {	
                                            p_mt_fsm_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
                                                        p_mt_fsm_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


                                                    }
                                                    else
                                                    {
                                                        p_mt_fsm_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
                                                    }
                                                }

                                                finished_flag++;

                                            }
                                        }

                                    }
                                }

                            }

                            index++;
                        }

                        else{
                            //fgets(line_val, MAX_LINE_BYTE, fp_apis);

                            //index++;
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

                            }
                            else if(index == 6)
                            {

                            }
                            else if(index == 7)
                            {
                                p_mt_fsm_req->arg.is_extension = 0;
                            }

                            index++;

                        }


                    }

                }


		fclose(fp_apis);

            }


            //expiry_time.tv_sec = 0;	
            //expiry_time.tv_usec = 0;

            for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
            {



                #if 0

                if (gettimeofday(&current_time,NULL) == 0)
                {
                    if (current_time.tv_sec > expiry_time.tv_sec || ( current_time.tv_sec == expiry_time.tv_sec && current_time.tv_usec >= expiry_time.tv_usec))
                    {

                        expiry_time.tv_sec =    current_time.tv_sec + (current_time.tv_usec + delay)/1000000;
                        expiry_time.tv_usec =   (current_time.tv_usec + delay)%1000000;

                #endif
                            for(message_counter = 0; message_counter < no_mess_in_burst;message_counter++ )
                            {

				    if(seq_control == 15)
					    seq_control = 1;
				    else
					    seq_control++;

                                send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
                                //map_memzero(send_api_temp,sizeof(map_api_struct_t));

                                p_mt_fsm_req_temp = (map_mt_forward_sm_request_t*)app_mem_get(sizeof(map_mt_forward_sm_request_t));
                                //map_memzero(p_mt_fsm_req_temp,sizeof(map_mt_forward_sm_request_t));



                                /*

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
                                */
                                    corr_id = app_map_get_new_correlation_id();
                                    p_mt_fsm_req->header.corr_id =  corr_id;

                                    send_open_req(api_name ,corr_id);
                                //}

                                memcpy(p_mt_fsm_req_temp,p_mt_fsm_req,sizeof(map_mt_forward_sm_request_t));
                                memcpy(send_api_temp,send_api_mtfsm,sizeof(map_api_struct_t));

                                p_mt_fsm_req_temp->header.invoke_id = message_counter + 1;

                                //if(message_counter == (num_of_messages -1))
                                p_mt_fsm_req_temp->header.last_component = 1;

                                send_api_temp->p_data = p_mt_fsm_req_temp;
                                app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

                                mt_request_counters++;

				if((mt_request_counters%50000) == 0)
					print_mt_report();

                            }


                            usleep(delay);

                    #if 0

                    }

                    else
                    {
                        //	printf("I m sleeping for [%d]",(((expiry_time.tv_sec*1000000) + expiry_time.tv_usec)  - (( current_time.tv_sec*1000000) + current_time.tv_usec))/1001);
                        usleep(((expiry_time.tv_sec*1000000) + expiry_time.tv_usec)  - (( current_time.tv_sec*1000000) + current_time.tv_usec));
                    }
                }

                    #endif

            }

            #if 0
			app_mem_free(p_mt_fsm_req);
			app_mem_free(send_api_mtfsm);
            #endif


		}
		else if(!strncmp(api_name,"MAP_V1_ALERT_SERVICE_CENTRE_REQUEST",strlen("MAP_V1_ALERT_SERVICE_CENTRE_REQUEST"))) 
		{

			index = 0;

			
			map_v1_alert_service_centre_without_result_request_t *p_alert_ser_centre_req = NULL,*p_alert_ser_centre_req_temp = NULL;
			p_alert_ser_centre_req = (map_v1_alert_service_centre_without_result_request_t *)\
			app_mem_get (sizeof (map_v1_alert_service_centre_without_result_request_t));
			map_memzero(p_alert_ser_centre_req, sizeof (map_v1_alert_service_centre_without_result_request_t));

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

					printf("MAP_ALERT_SERVICE_CENTRE_REQUEST  is successfully Parsed\n\n");
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
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
						corr_id = app_map_get_new_correlation_id();
						p_alert_ser_centre_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_alert_ser_centre_req_temp,p_alert_ser_centre_req,sizeof (map_v1_alert_service_centre_without_result_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_alert_ser_centre_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
						p_alert_ser_centre_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_alert_ser_centre_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_alert_ser_centre_req);


		}
		else if(!strncmp(api_name,"MAP_V2_ALERT_SERVICE_CENTRE_REQUEST",strlen("MAP_V2_ALERT_SERVICE_CENTRE_REQUEST"))) 
		{

			index = 0;

			
			map_v2_alert_service_centre_request_t *p_alert_ser_centre_req = NULL,*p_alert_ser_centre_req_temp = NULL;
			p_alert_ser_centre_req = (map_v2_alert_service_centre_request_t *)\
			app_mem_get (sizeof (map_v2_alert_service_centre_request_t));
			map_memzero(p_alert_ser_centre_req, sizeof (map_v2_alert_service_centre_request_t));

			fill_api_header(&(send_api->header), hlr_user_id);
			send_api->header.api_id = MAP_ALERT_SERVICE_CENTRE_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 2;
			send_api->header.len = sizeof(map_v2_alert_service_centre_request_t);

			fill_header(&p_alert_ser_centre_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_ALERT_SERVICE_CENTRE_REQUEST  is successfully Parsed\n\n");
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_alert_ser_centre_req_temp = (map_v2_alert_service_centre_request_t *)\
					app_mem_get(sizeof (map_v2_alert_service_centre_request_t));
					map_memzero(p_alert_ser_centre_req_temp, sizeof (map_v2_alert_service_centre_request_t));



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
						corr_id = app_map_get_new_correlation_id();
						p_alert_ser_centre_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_alert_ser_centre_req_temp,p_alert_ser_centre_req,sizeof (map_v2_alert_service_centre_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_alert_ser_centre_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
						p_alert_ser_centre_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_alert_ser_centre_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_alert_ser_centre_req);


		}
		else if(!strncmp(api_name,"MAP_V2_INFORM_SERVICE_CENTRE_REQUEST",strlen("MAP_V2_INFORM_SERVICE_CENTRE_REQUEST"))) 
		{

			index = 0;

			send_api = app_mem_get(sizeof(map_api_struct_t));
			map_memzero(send_api,sizeof(map_api_struct_t));			

			map_v2_inform_service_centre_request_t *p_inform_ser_centre_req = NULL,*p_inform_ser_centre_req_temp = NULL;
			p_inform_ser_centre_req = (map_v2_inform_service_centre_request_t *)\
			app_mem_get (sizeof (map_v2_inform_service_centre_request_t));
			map_memzero(p_inform_ser_centre_req, sizeof (map_v2_inform_service_centre_request_t));

			fill_api_header(&(send_api->header), msc_user_id);
			send_api->header.api_id = MAP_INFORM_SERVICE_CENTRE_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 2;
			send_api->header.len = sizeof(map_v2_inform_service_centre_request_t);

			fill_header(&p_inform_ser_centre_req->header,corr_id);



				sprintf(filename,"../buffers/%s",api_name);
				fp_apis = fopen(filename,"r");


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_V2_INFORM_SERVICE_CENTRE_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)    
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
								p_inform_ser_centre_req->arg.is_stored_msisdn = atoi(token_generic);
							}
							else if(index == 1)
							{

								p_inform_ser_centre_req->arg.stored_msisdn.length = atoi(token_length);
								p_inform_ser_centre_req->arg.stored_msisdn.value[counter] = hstoi(token_generic);
							}
							else if(index == 2)
							{

								p_inform_ser_centre_req->arg.is_mw_status = atoi(token_generic);
							}
							else if(index == 3)
							{
								p_inform_ser_centre_req->arg.mw_status.length =1;
								//p_inform_ser_centre_req->arg.mw_status.length = atoi(token_length) * 8;
								//p_inform_ser_centre_req->arg.mw_status.length =48;
								p_inform_ser_centre_req->arg.mw_status.value[counter] = hstoi(token_generic);
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message V2 INFORM SERVICE CENTRE ####\n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_inform_ser_centre_req_temp = (map_v2_inform_service_centre_request_t *)\
					app_mem_get(sizeof (map_v2_inform_service_centre_request_t));
					map_memzero(p_inform_ser_centre_req_temp, sizeof (map_v2_inform_service_centre_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_inform_ser_centre_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_inform_ser_centre_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_inform_ser_centre_req_temp,p_inform_ser_centre_req,sizeof (map_v2_inform_service_centre_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_inform_ser_centre_req_temp->header.invoke_id = message_counter + 1;

					//if(message_counter == (num_of_messages -1))
						p_inform_ser_centre_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_inform_ser_centre_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

					sleep(5);
				}

			}

			app_mem_free(send_api);
			app_mem_free(p_inform_ser_centre_req);


		}
		else if(!strncmp(api_name,"MAP_INFORM_SERVICE_CENTRE_REQUEST",strlen("MAP_INFORM_SERVICE_CENTRE_REQUEST"))) 
		{

			index = 0;

			
			map_inform_service_centre_request_t *p_inform_ser_centre_req = NULL,*p_inform_ser_centre_req_temp = NULL;

			send_api = app_mem_get(sizeof(map_api_struct_t));
			map_memzero(send_api,sizeof(map_api_struct_t));

			p_inform_ser_centre_req = (map_inform_service_centre_request_t *)\
			app_mem_get (sizeof (map_inform_service_centre_request_t));
			map_memzero(p_inform_ser_centre_req, sizeof (map_inform_service_centre_request_t));

			fill_api_header(&(send_api->header), msc_user_id);
			send_api->header.api_id = MAP_INFORM_SERVICE_CENTRE_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 3;
			send_api->header.len = sizeof(map_inform_service_centre_request_t);

			fill_header(&p_inform_ser_centre_req->header,corr_id);

			sprintf(filename,"../buffers/%s",api_name);
			fp_apis = fopen(filename,"r");


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_INFORM_SERVICE_CENTRE_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

						token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
								p_inform_ser_centre_req->arg.is_stored_msisdn = atoi(token_generic);
							}
							else if(index == 1)
							{

								p_inform_ser_centre_req->arg.stored_msisdn.length = atoi(token_length);
								p_inform_ser_centre_req->arg.stored_msisdn.value[counter] = hstoi(token_generic);
							}
							else if(index == 2)
							{

								p_inform_ser_centre_req->arg.is_mw_status = atoi(token_length);
							}
							else if(index == 3)
							{
								p_inform_ser_centre_req->arg.mw_status.length = atoi(token_length)* 8;
								p_inform_ser_centre_req->arg.mw_status.value[counter] = hstoi(token_generic);
							}
							else if(index == 4)
							{

								p_inform_ser_centre_req->arg.is_extension = atoi(token_generic);
							}
							else if(index == 5)
							{
								p_inform_ser_centre_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter <p_inform_ser_centre_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_inform_ser_centre_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_inform_ser_centre_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 6)
							{
								
							  for(ext_counter = 0; ext_counter < p_inform_ser_centre_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_inform_ser_centre_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_inform_ser_centre_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_inform_ser_centre_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_inform_ser_centre_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
							}
							else if(index == 7)
							{
								p_inform_ser_centre_req->arg.is_absent_subscriber_diagnostic_sm = atoi(token_generic);
							}
							else if(index ==8)
							{
								p_inform_ser_centre_req->arg.absent_subscriber_diagnostic_sm = hstoi(token_generic);

							}
							else if(index == 9)
							{
								p_inform_ser_centre_req->arg.is_additional_absent_subscriber_diagnostic_sm = atoi(token_generic);

							}
							else if(index == 10)
							{
								p_inform_ser_centre_req->arg.additional_absent_subscriber_diagnostic_sm = hstoi(token_generic);

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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_inform_ser_centre_req_temp = (map_inform_service_centre_request_t *)\
					app_mem_get(sizeof (map_inform_service_centre_request_t));
					map_memzero(p_inform_ser_centre_req_temp, sizeof (map_inform_service_centre_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_inform_ser_centre_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_inform_ser_centre_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_inform_ser_centre_req_temp,p_inform_ser_centre_req,sizeof (map_inform_service_centre_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_inform_ser_centre_req_temp->header.invoke_id = message_counter + 1;

					//if(message_counter == (num_of_messages -1))
						p_inform_ser_centre_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_inform_ser_centre_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				sleep(5);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_inform_ser_centre_req);


		}
		else if(!strncmp(api_name,"MAP_REPORT_SM_DELIVERY_STATUS_REQUEST",strlen("MAP_REPORT_SM_DELIVERY_STATUS_REQUEST"))) 
		{

			index = 0;

			
			map_report_sm_delivery_status_request_t *p_report_sm_delivery_req = NULL,*p_report_sm_delivery_req_temp = NULL;
			p_report_sm_delivery_req = (map_report_sm_delivery_status_request_t *)\
			app_mem_get (sizeof (map_report_sm_delivery_status_request_t));
			map_memzero(p_report_sm_delivery_req, sizeof (map_report_sm_delivery_status_request_t));

			fill_api_header(&(send_api->header), msc_user_id);
			send_api->header.api_id = MAP_REPORT_SM_DELIVERY_STATUS_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 3;
			send_api->header.len = sizeof(map_report_sm_delivery_status_request_t);

			fill_header(&p_report_sm_delivery_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_REPORT_SM_DELIVERY_STATUS_REQUET  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

						token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
								p_report_sm_delivery_req->arg.msisdn.length = atoi(token_length);
								p_report_sm_delivery_req->arg.msisdn.value[counter] = hstoi(token_generic);
							}
							else if(index == 1)
							{

								p_report_sm_delivery_req->arg.service_centre_address.length = atoi(token_length);
								p_report_sm_delivery_req->arg.service_centre_address.value[counter] = hstoi(token_generic);
							}
							else if(index == 2)
							{

								p_report_sm_delivery_req->arg.sm_delivery_outcome = atoi(token_generic);
							}
							else if(index == 3)
							{
								p_report_sm_delivery_req->arg.is_absent_subscriber_diagnostic_sm = atoi(token_generic);
							}
							else if(index == 4)
							{
								
								p_report_sm_delivery_req->arg.absent_subscriber_diagnostic_sm = hstoi(token_generic);
							}
							else if(index == 5)
							{

								p_report_sm_delivery_req->arg.is_extension = atoi(token_generic);
							}
							else if(index == 6)
							{
								p_report_sm_delivery_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter <p_report_sm_delivery_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_report_sm_delivery_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_report_sm_delivery_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 7)
							{
								
							  for(ext_counter = 0; ext_counter < p_report_sm_delivery_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_report_sm_delivery_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_report_sm_delivery_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_report_sm_delivery_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_report_sm_delivery_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
							}
							else if(index == 8)
								p_report_sm_delivery_req->arg.is_gprs_support_indicator = atoi(token_generic);
							else if(index ==9)
								p_report_sm_delivery_req->arg.gprs_support_indicator = atoi(token_generic);

							else if(index == 10)
								p_report_sm_delivery_req->arg.is_delivery_outcome_indicator = atoi(token_generic);

							else if(index == 11)
								p_report_sm_delivery_req->arg.delivery_outcome_indicator = atoi(token_generic);

							else if(index == 12)
								p_report_sm_delivery_req->arg.is_additional_sm_delivery_outcome = atoi(token_generic);
							else if(index == 13)
								p_report_sm_delivery_req->arg.additional_sm_delivery_outcome = atoi(token_generic);
							else if(index == 14)
								p_report_sm_delivery_req->arg.is_additional_absent_subscriber_diagnostic_sm = atoi(token_generic);
							else if(index == 15)
								p_report_sm_delivery_req->arg.additional_absent_subscriber_diagnostic_sm = atoi(token_generic);




						}

						index++;

					}
					else{

						printf("No Optional Parameter is there so Please check\n");
						break;
					}

				}

			}


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_report_sm_delivery_req_temp = (map_report_sm_delivery_status_request_t *)\
					app_mem_get(sizeof (map_report_sm_delivery_status_request_t));
					map_memzero(p_report_sm_delivery_req_temp, sizeof (map_report_sm_delivery_status_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_report_sm_delivery_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_report_sm_delivery_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_report_sm_delivery_req_temp,p_report_sm_delivery_req,sizeof (map_report_sm_delivery_status_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_report_sm_delivery_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
						p_report_sm_delivery_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_report_sm_delivery_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_report_sm_delivery_req);


		}

        else
        if(!strncmp(api_name,"MAP_V2_REPORT_SM_DELIVERY_STATUS_REQUEST",strlen("MAP_V2_REPORT_SM_DELIVERY_STATUS_REQUEST"))) 
		{

			index = 0;

			
			map_v2_report_sm_delivery_status_request_t *p_report_sm_delivery_req = NULL,*p_report_sm_delivery_req_temp = NULL;
			p_report_sm_delivery_req = (map_v2_report_sm_delivery_status_request_t *)\
			app_mem_get (sizeof (map_v2_report_sm_delivery_status_request_t));
			map_memzero(p_report_sm_delivery_req, sizeof (map_v2_report_sm_delivery_status_request_t));

			fill_api_header(&(send_api->header), msc_user_id);
			send_api->header.api_id = MAP_REPORT_SM_DELIVERY_STATUS_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 2;
			send_api->header.len = sizeof(map_v2_report_sm_delivery_status_request_t);

			fill_header(&p_report_sm_delivery_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_REPORT_SM_DELIVERY_STATUS_REQUET  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

						token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
								p_report_sm_delivery_req->arg.msisdn.length = atoi(token_length);
								p_report_sm_delivery_req->arg.msisdn.value[counter] = hstoi(token_generic);
							}
							else if(index == 1)
							{

								p_report_sm_delivery_req->arg.service_centre_address.length = atoi(token_length);
								p_report_sm_delivery_req->arg.service_centre_address.value[counter] = hstoi(token_generic);
							}
                            else if(index == 2)
                                p_report_sm_delivery_req->arg.is_sm_delivery_outcome = atoi(token_generic);
							else if(index == 3)
							{

								p_report_sm_delivery_req->arg.sm_delivery_outcome = atoi(token_generic);
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_report_sm_delivery_req_temp = ( map_v2_report_sm_delivery_status_request_t *)\
					app_mem_get(sizeof (map_v2_report_sm_delivery_status_request_t));
					map_memzero(p_report_sm_delivery_req_temp, sizeof (map_v2_report_sm_delivery_status_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_report_sm_delivery_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_report_sm_delivery_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_report_sm_delivery_req_temp,p_report_sm_delivery_req,sizeof (map_report_sm_delivery_status_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_report_sm_delivery_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
						p_report_sm_delivery_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_report_sm_delivery_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_report_sm_delivery_req);


		}

		else if(!strncmp(api_name,"MAP_V1_SEND_ROUTING_INFORMATION_REQUEST",strlen("MAP_V1_SEND_ROUTING_INFORMATION_REQUEST"))) 
		{

			index = 0;

			
			map_v1_send_routing_information_request_t *p_send_routing_info_req = NULL,*p_send_routing_info_req_temp = NULL;
			p_send_routing_info_req = (map_v1_send_routing_information_request_t *)\
			app_mem_get (sizeof (map_v1_send_routing_information_request_t));
			map_memzero(p_send_routing_info_req, sizeof (map_v1_send_routing_information_request_t));

			fill_api_header(&(send_api->header), msc_user_id);
			send_api->header.api_id = MAP_SEND_ROUTING_INFORMATION_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 1;
			send_api->header.len = sizeof(map_v1_send_routing_information_request_t);

			fill_header(&p_send_routing_info_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_V1_SEND_ROUTING_INFORMATION_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
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
								p_send_routing_info_req->arg.ms_isdn.length = atoi(token_length);
								p_send_routing_info_req->arg.ms_isdn.value[counter] = hstoi(token_generic);
							}
							else if(index == 1)
								p_send_routing_info_req->arg.is_cug_interlock = atoi(token_generic);
							else if(index == 2)
							{
								p_send_routing_info_req->arg.cug_interlock.length = atoi(token_length);
								p_send_routing_info_req->arg.cug_interlock.value[counter] = atoi(token_generic);
							}
							else if(index == 3)
								p_send_routing_info_req->arg.is_v1_number_of_forwarding = atoi(token_generic);
							else if(index == 4)
								p_send_routing_info_req->arg.v1_number_of_forwarding = hstoi(token_generic); 		
							else if(index == 5)
								
								p_send_routing_info_req->arg.is_network_signal_info = atoi(token_generic);
							else if(index == 6)
								p_send_routing_info_req->arg.network_signal_info.protocol_id = atoi(token_generic);

							else if(index == 7)
							{
								p_send_routing_info_req->arg.network_signal_info.signal_info.length = atoi(token_length);
								p_send_routing_info_req->arg.network_signal_info.signal_info.value[counter] = hstoi(token_generic);
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_send_routing_info_req_temp = (map_v1_send_routing_information_request_t *)\
					app_mem_get(sizeof (map_v1_send_routing_information_request_t));
					map_memzero(p_send_routing_info_req_temp, sizeof (map_v1_send_routing_information_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_send_routing_info_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_send_routing_info_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_send_routing_info_req_temp,p_send_routing_info_req,sizeof (map_v1_send_routing_information_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_send_routing_info_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
						p_send_routing_info_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_send_routing_info_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_send_routing_info_req);


		}
        else if(!strncmp(api_name,"MAP_V2_SEND_ROUTING_INFORMATION_REQUEST",strlen("MAP_V2_SEND_ROUTING_INFORMATION_REQUEST"))) 
		{

			index = 0;

			map_v2_send_routing_info_request_t *p_send_routing_info_req = NULL,*p_send_routing_info_req_temp = NULL;
			p_send_routing_info_req = (map_v2_send_routing_info_request_t *)\
			app_mem_get (sizeof (map_v2_send_routing_info_request_t));
			map_memzero(p_send_routing_info_req, sizeof (map_v2_send_routing_info_request_t));

			fill_api_header(&(send_api->header), msc_user_id);
			send_api->header.api_id = MAP_SEND_ROUTING_INFORMATION_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 2;
			send_api->header.len = sizeof(map_v2_send_routing_info_request_t);

			fill_header(&p_send_routing_info_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_V2_SEND_ROUTING_INFORMATION_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
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
								p_send_routing_info_req->arg.msisdn.length = atoi(token_length);
								p_send_routing_info_req->arg.msisdn.value[counter] = hstoi(token_generic);
							}
							else if(index == 1)
								p_send_routing_info_req->arg.is_cug_check_info = atoi(token_generic);
							else if(index == 2)
							{
								p_send_routing_info_req->arg.cug_check_info.cug_interlock.length = atoi(token_length);
								p_send_routing_info_req->arg.cug_check_info.cug_interlock.value[counter] = hstoi(token_generic);
							}
                            else if(index == 3)
                                p_send_routing_info_req->arg.cug_check_info.is_cug_outgoing_access= atoi(token_length);
                            else if(index == 4)
                                p_send_routing_info_req->arg.cug_check_info.cug_outgoing_access = atoi(token_length);
							else if(index == 5)
								p_send_routing_info_req->arg.is_number_of_forwarding = atoi(token_generic);
							else if(index == 6)
								p_send_routing_info_req->arg.number_of_forwarding = hstoi(token_generic); 		
							else if(index == 7)
								
								p_send_routing_info_req->arg.is_network_signal_info = atoi(token_generic);
							else if(index == 8)
								p_send_routing_info_req->arg.network_signal_info.protocol_id = atoi(token_generic);

							else if(index == 9)
							{
								p_send_routing_info_req->arg.network_signal_info.signal_info.length = atoi(token_length);
								p_send_routing_info_req->arg.network_signal_info.signal_info.value[counter] = hstoi(token_generic);
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_send_routing_info_req_temp = (map_v2_send_routing_info_request_t *)\
					app_mem_get(sizeof (map_v2_send_routing_info_request_t));
					map_memzero(p_send_routing_info_req_temp, sizeof (map_v2_send_routing_info_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_send_routing_info_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_send_routing_info_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_send_routing_info_req_temp,p_send_routing_info_req,sizeof (map_v1_send_routing_information_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_send_routing_info_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
						p_send_routing_info_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_send_routing_info_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_send_routing_info_req);


		}

		else if(!strncmp(api_name,"MAP_SEND_ROUTING_INFORMATION_REQUEST",strlen("MAP_SEND_ROUTING_INFORMATION_REQUEST"))) 
		{

			index = 0;
			if(send_api_sri == NULL)
			{
				send_api_sri = app_mem_get(sizeof(map_api_struct_t));
				map_memzero(send_api_sri,sizeof(map_api_struct_t));

				sprintf(filename,"../buffers/%s",api_name);
				fp_apis = fopen(filename,"r");
				
			}
			

			if(p_send_routing_info_req == NULL)
			{
				p_send_routing_info_req = (map_send_routing_info_request_t *)\
							  app_mem_get (sizeof (map_send_routing_info_request_t));
				map_memzero(p_send_routing_info_req, sizeof (map_send_routing_info_request_t));

				fill_api_header(&(send_api_sri->header), msc_user_id);
				send_api_sri->header.api_id = MAP_SEND_ROUTING_INFORMATION_REQUEST;
				send_api_sri->header.spare1 = g_sap;
				send_api_sri->header.ver = 3;
				send_api_sri->header.len = sizeof(map_send_routing_info_request_t);

				fill_header(&p_send_routing_info_req->header,corr_id);


				fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
				memset(bitmap_array,'\0',20);
				strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_SEND_ROUTING_INFORMATION_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
					if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

						token_val_temp = (char *)malloc(strlen(token_val));
						strncpy(token_val_temp,token_val,strlen(token_val));

						//printf("Token Length [%s] \n",token_length);
						//printf("Token Value [%s] \n",token_val);
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
								p_send_routing_info_req->arg.msisdn.length = atoi(token_length);
								p_send_routing_info_req->arg.msisdn.value[counter] = hstoi(token_generic);

								/* For testing Purpose Only */


								p_send_routing_info_req->arg.is_cug_check_info = 0;
								p_send_routing_info_req->arg.is_number_of_forwarding = 0;
								p_send_routing_info_req->arg.interrogation_type = 0;
								p_send_routing_info_req->arg.is_or_interrogation = 0;
								p_send_routing_info_req->arg.is_or_capability = 0;


								p_send_routing_info_req->arg.is_call_reference_number = 0;

								p_send_routing_info_req->arg.is_forwarding_reason = 0;

								p_send_routing_info_req->arg.is_basic_service_group = 0;

								p_send_routing_info_req->arg.is_network_signal_info = 0;
								p_send_routing_info_req->arg.is_camel_info = 0;
								p_send_routing_info_req->arg.is_suppression_of_announcement = 0;

								p_send_routing_info_req->arg.is_extension = 0;
								p_send_routing_info_req->arg.is_alerting_pattern = 0;
								p_send_routing_info_req->arg.is_ccbs_call = 0;
								p_send_routing_info_req->arg.is_supported_ccbs_phase = 0;
								p_send_routing_info_req->arg.is_additional_signal_info= 0;
								p_send_routing_info_req->arg.is_ist_support_indicator = 0;
								p_send_routing_info_req->arg.is_pre_paging_supported = 0;
								p_send_routing_info_req->arg.is_call_diversion_treatment_indicator = 0;
								p_send_routing_info_req->arg.is_long_ftn_supported = 0;
								p_send_routing_info_req->arg.is_suppress_vt_csi = 0;
								p_send_routing_info_req->arg.is_suppress_incoming_call_barring = 0;
								p_send_routing_info_req->arg.is_gsm_scf_initiated_call = 0;
								p_send_routing_info_req->arg.is_basic_service_group2 = 0;


								p_send_routing_info_req->arg.is_network_signal_info2 = 0;


							}
							else if(index == 1)
								p_send_routing_info_req->arg.is_cug_check_info = atoi(token_generic);
							else if(index == 2)
							{
								p_send_routing_info_req->arg.cug_check_info.cug_interlock.length = atoi(token_length);
								p_send_routing_info_req->arg.cug_check_info.cug_interlock.value[counter] = hstoi(token_generic);
							}
							else if(index ==3)
								p_send_routing_info_req->arg.cug_check_info.is_cug_outgoing_access = atoi(token_generic);
							else if(index ==4)
								p_send_routing_info_req->arg.cug_check_info.cug_outgoing_access = hstoi(token_generic);
							else if(index == 5)
							{

								p_send_routing_info_req->arg.cug_check_info.is_extension = atoi(token_generic);
							}
							else if(index == 6)
							{
								p_send_routing_info_req->arg.cug_check_info.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter <p_send_routing_info_req->arg.cug_check_info.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_send_routing_info_req->arg.cug_check_info.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_send_routing_info_req->arg.cug_check_info.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}


								}

							}
							else if(index == 7)
							{

								for(ext_counter = 0; ext_counter < p_send_routing_info_req->arg.cug_check_info.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
									}		
									else{
										token_ext = strtok(NULL,":");
									}


									if(!strncmp(token_ext,"NULL",strlen("NULL")))
										p_send_routing_info_req->arg.cug_check_info.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
									else
									{	
										p_send_routing_info_req->arg.cug_check_info.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
													p_send_routing_info_req->arg.cug_check_info.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


												}
												else
												{
													p_send_routing_info_req->arg.cug_check_info.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
												}
											}

											finished_flag++;

										}
									}

								}
							}

							else if(index == 8)
								p_send_routing_info_req->arg.is_number_of_forwarding = atoi(token_generic);
							else if(index == 9)
								p_send_routing_info_req->arg.number_of_forwarding = hstoi(token_generic); 		
							else if(index == 10)

								p_send_routing_info_req->arg.interrogation_type = atoi(token_generic);
							else if(index == 11)
								p_send_routing_info_req->arg.is_or_interrogation = atoi(token_generic);
							else if(index == 12)
								p_send_routing_info_req->arg.or_interrogation = hstoi(token_generic);
							else if(index == 13)
								p_send_routing_info_req->arg.is_or_capability = atoi(token_generic);
							else if(index == 14)
								p_send_routing_info_req->arg.or_capability = hstoi(token_generic);
							else if(index == 15)
							{
								p_send_routing_info_req->arg.gmsc_or_gsm_scf_address.length = atoi(token_length);
								p_send_routing_info_req->arg.gmsc_or_gsm_scf_address.value[counter] = hstoi(token_generic);
							}

							else if(index == 16)
								p_send_routing_info_req->arg.is_call_reference_number = atoi(token_generic);
							else if(index == 17)
							{
								p_send_routing_info_req->arg.call_reference_number.length = atoi(token_length);
								p_send_routing_info_req->arg.call_reference_number.value[counter] = hstoi(token_generic);
							}
							else if(index == 18)
								p_send_routing_info_req->arg.is_forwarding_reason = atoi(token_generic);
							else if(index == 19)
								p_send_routing_info_req->arg.forwarding_reason = atoi(token_generic);
							else if(index == 20)
								p_send_routing_info_req->arg.is_basic_service_group = atoi(token_generic);
							else if(index == 21)
								p_send_routing_info_req->arg.basic_service_group.choice = atoi(token_generic);
							else if(index == 22)
							{
								if(1 == p_send_routing_info_req->arg.basic_service_group.choice )
								{
									p_send_routing_info_req->arg.basic_service_group.u.ext_bearer_service.length = atoi(token_length);
									p_send_routing_info_req->arg.basic_service_group.u.ext_bearer_service.value[counter] = atoi(token_generic);
								}
								else if(2 ==p_send_routing_info_req->arg.basic_service_group.choice)
								{
									p_send_routing_info_req->arg.basic_service_group.u.ext_teleservice.length = atoi(token_length);
									p_send_routing_info_req->arg.basic_service_group.u.ext_teleservice.value[counter] = atoi(token_generic);

								}

							}
							else if(index == 23)
								p_send_routing_info_req->arg.is_network_signal_info = atoi(token_generic);
							else if(index == 24)
								p_send_routing_info_req->arg.network_signal_info.protocol_id= atoi(token_generic);
							else if(index == 25)
							{
								p_send_routing_info_req->arg.network_signal_info.signal_info.length = atoi(token_length);
								p_send_routing_info_req->arg.network_signal_info.signal_info.value[counter] = atoi(token_generic);

							}
							else if(index == 26)
							{

								p_send_routing_info_req->arg.is_extension = atoi(token_generic);
							}
							else if(index == 27)
							{
								p_send_routing_info_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter <p_send_routing_info_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_send_routing_info_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_send_routing_info_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}


								}

							}
							else if(index == 28)
							{

								for(ext_counter = 0; ext_counter < p_send_routing_info_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
									}		
									else{
										token_ext = strtok(NULL,":");
									}


									if(!strncmp(token_ext,"NULL",strlen("NULL")))
										p_send_routing_info_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
									else
									{	
										p_send_routing_info_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
													p_send_routing_info_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


												}
												else
												{
													p_send_routing_info_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
												}
											}

											finished_flag++;

										}
									}

								}
							}

							else if(index == 29)
								p_send_routing_info_req->arg.is_camel_info = atoi(token_generic);
							else if(index == 30)
							{
								p_send_routing_info_req->arg.camel_info.supported_camel_phases.length = atoi(token_length);
								p_send_routing_info_req->arg.camel_info.supported_camel_phases.value[counter]= hstoi(token_generic);
							}		
							else if(index == 31)
								p_send_routing_info_req->arg.camel_info.is_suppress_t_csi = atoi(token_generic);
							else if(index == 32)
								p_send_routing_info_req->arg.camel_info.suppress_t_csi = hstoi(token_generic);

							else if(index == 33)
							{

								p_send_routing_info_req->arg.camel_info.is_extension = atoi(token_generic);
							}
							else if(index == 34)
							{
								p_send_routing_info_req->arg.camel_info.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter <p_send_routing_info_req->arg.camel_info.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_send_routing_info_req->arg.camel_info.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_send_routing_info_req->arg.camel_info.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}


								}

							}
							else if(index == 35)
							{

								for(ext_counter = 0; ext_counter < p_send_routing_info_req->arg.camel_info.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
									}		
									else{
										token_ext = strtok(NULL,":");
									}


									if(!strncmp(token_ext,"NULL",strlen("NULL")))
										p_send_routing_info_req->arg.camel_info.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
									else
									{	
										p_send_routing_info_req->arg.camel_info.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
													p_send_routing_info_req->arg.camel_info.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


												}
												else
												{
													p_send_routing_info_req->arg.camel_info.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
												}
											}

											finished_flag++;

										}
									}

								}
							}

							else if(index == 36)
								p_send_routing_info_req->arg.camel_info.is_offered_camel4_csis = atoi(token_generic);
							else if(index == 37)
							{
								p_send_routing_info_req->arg.camel_info.offered_camel4_csis.length = atoi(token_length);
								p_send_routing_info_req->arg.camel_info.offered_camel4_csis.value[counter] = hstoi(token_generic);
							}

							else if(index == 38)
								p_send_routing_info_req->arg.is_suppression_of_announcement = atoi(token_generic);
							else if(index == 39)
								p_send_routing_info_req->arg.suppression_of_announcement = hstoi(token_generic);
							else if(index == 40)
							{

								p_send_routing_info_req->arg.is_extension = atoi(token_generic);
							}
							else if(index == 41)
							{
								p_send_routing_info_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter <p_send_routing_info_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_send_routing_info_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_send_routing_info_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}


								}

							}
							else if(index == 42)
							{

								for(ext_counter = 0; ext_counter < p_send_routing_info_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
									}		
									else{
										token_ext = strtok(NULL,":");
									}


									if(!strncmp(token_ext,"NULL",strlen("NULL")))
										p_send_routing_info_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
									else
									{	
										p_send_routing_info_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
													p_send_routing_info_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


												}
												else
												{
													p_send_routing_info_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
												}
											}

											finished_flag++;

										}
									}

								}
							}
							else if(index == 43)	
								p_send_routing_info_req->arg.is_alerting_pattern = atoi(token_generic);
							else if(index == 44)	
							{
								p_send_routing_info_req->arg.alerting_pattern.length = atoi(token_length);
								p_send_routing_info_req->arg.alerting_pattern.value[counter] = hstoi(token_generic);
							}
							else if(index == 45)
								p_send_routing_info_req->arg.is_ccbs_call = atoi(token_generic);
							else if(index == 46)
								p_send_routing_info_req->arg.ccbs_call = hstoi(token_generic);
							else if(index == 47)
								p_send_routing_info_req->arg.is_supported_ccbs_phase = atoi(token_generic);
							else if(index == 48)
								p_send_routing_info_req->arg.supported_ccbs_phase = hstoi(token_generic);

							else if(index == 49)
								p_send_routing_info_req->arg.is_additional_signal_info = atoi(token_generic);
							else if(index == 50)
								p_send_routing_info_req->arg.additional_signal_info.ext_protocol_id = atoi(token_generic);
							else if(index == 51)
							{
								p_send_routing_info_req->arg.additional_signal_info.signal_info.length = atoi(token_length);
								p_send_routing_info_req->arg.additional_signal_info.signal_info.value[counter] = atoi(token_generic);
							}
							else if(index == 52)
							{

								p_send_routing_info_req->arg.additional_signal_info.is_extension = atoi(token_generic);
							}
							else if(index == 53)
							{
								p_send_routing_info_req->arg.additional_signal_info.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter <p_send_routing_info_req->arg.additional_signal_info.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_send_routing_info_req->arg.additional_signal_info.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_send_routing_info_req->arg.additional_signal_info.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}


								}

							}
							else if(index == 54)
							{

								for(ext_counter = 0; ext_counter < p_send_routing_info_req->arg.additional_signal_info.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
									}		
									else{
										token_ext = strtok(NULL,":");
									}


									if(!strncmp(token_ext,"NULL",strlen("NULL")))
										p_send_routing_info_req->arg.additional_signal_info.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
									else
									{	
										p_send_routing_info_req->arg.additional_signal_info.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
													p_send_routing_info_req->arg.additional_signal_info.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


												}
												else
												{
													p_send_routing_info_req->arg.additional_signal_info.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
												}
											}

											finished_flag++;

										}
									}

								}
							}

							else if(index == 55)
								p_send_routing_info_req->arg.is_ist_support_indicator = atoi(token_generic);
							else if(index == 56)
								p_send_routing_info_req->arg.ist_support_indicator = atoi(token_generic);
							else if(index == 57)
								p_send_routing_info_req->arg.is_pre_paging_supported = atoi(token_generic);
							else if(index == 58)
								p_send_routing_info_req->arg.pre_paging_supported = hstoi(token_generic);
							else if(index == 59)
								p_send_routing_info_req->arg.is_call_diversion_treatment_indicator = atoi(token_generic);
							else if(index == 60)
							{
								p_send_routing_info_req->arg.call_diversion_treatment_indicator.length = atoi(token_length);
								p_send_routing_info_req->arg.call_diversion_treatment_indicator.value[counter] = hstoi(token_generic);
							}
							else if(index == 61)
								p_send_routing_info_req->arg.is_long_ftn_supported  = atoi(token_generic);
							else if(index == 62)
								p_send_routing_info_req->arg.long_ftn_supported  = hstoi(token_generic);
							else if(index == 63)
								p_send_routing_info_req->arg.is_suppress_vt_csi  = atoi(token_generic);
							else if(index == 64)
								p_send_routing_info_req->arg.suppress_vt_csi  = hstoi(token_generic);

							else if(index == 65)
								p_send_routing_info_req->arg.is_suppress_incoming_call_barring  = atoi(token_generic);
							else if(index == 66)
								p_send_routing_info_req->arg.suppress_incoming_call_barring  = hstoi(token_generic);
							else if(index == 67)
								p_send_routing_info_req->arg.is_gsm_scf_initiated_call  = atoi(token_generic);
							else if(index == 68)
								p_send_routing_info_req->arg.gsm_scf_initiated_call  = hstoi(token_generic);

#if 0
							else if(index == 69)

								p_send_routing_info_req->arg.is_basic_service_group2  = atoi(token_generic);
							else if(index == 70)
								p_send_routing_info_req->arg.basic_service_group2.choice = atoi(token_generic);
							else if(index == 71)
							{
								if( 1 == p_send_routing_info_req->arg.basic_service_group2.choice )
								{
									p_send_routing_info_req->arg.basic_service_group2.u.ext_bearer_service.length = atoi(token_length);
									p_send_routing_info_req->arg.basic_service_group2.u.ext_bearer_service.value[counter] = hstoi(token_generic);
								}
								else if( 2 == p_send_routing_info_req->arg.basic_service_group2.choice )
								{
									p_send_routing_info_req->arg.basic_service_group2.u.ext_teleservice.length = atoi(token_length);
									p_send_routing_info_req->arg.basic_service_group2.u.ext_teleservice.value[counter] = hstoi(token_generic);
								}
							}

							else if(index == 72)
								p_send_routing_info_req->arg.is_network_signal_info2 = atoi(token_generic);
							else if(index == 73)
								p_send_routing_info_req->arg.network_signal_info2.protocol_id = atoi(token_generic);
							else if(index == 74)
							{
								p_send_routing_info_req->arg.network_signal_info2.signal_info.length = atoi(token_length);
								p_send_routing_info_req->arg.network_signal_info2.signal_info.value[counter] = hstoi(token_generic);
							}
							else if(index == 75)
							{

								p_send_routing_info_req->arg.network_signal_info2.is_extension = atoi(token_generic);
							}

							else if(index == 76)
							{
								p_send_routing_info_req->arg.network_signal_info2.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter <p_send_routing_info_req->arg.network_signal_info2.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_send_routing_info_req->arg.network_signal_info2.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_send_routing_info_req->arg.network_signal_info2.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}


								}

							}
							else if(index == 77)
							{

								for(ext_counter = 0; ext_counter < p_send_routing_info_req->arg.network_signal_info2.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
									}		
									else{
										token_ext = strtok(NULL,":");
									}


									if(!strncmp(token_ext,"NULL",strlen("NULL")))
										p_send_routing_info_req->arg.network_signal_info2.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
									else
									{	
										p_send_routing_info_req->arg.network_signal_info2.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
													p_send_routing_info_req->arg.network_signal_info2.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


												}
												else
												{
													p_send_routing_info_req->arg.network_signal_info2.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
												}
											}

											finished_flag++;

										}
									}

								}
							}

#endif



						}	

						index++;

					//	free(token_val_temp);

					}
					else{

						printf("No Optional Parameter is there so Please check\n");
						break;
					}

				}

			}


				fclose(fp_apis);
			}


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < no_mess_in_burst;message_counter++ )
				{

					if(seq_control == 15)
						seq_control = 1;
					else
						seq_control++;

					send_api_sri_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					//map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_send_routing_info_req_temp = (map_send_routing_info_request_t *)\
					app_mem_get(sizeof (map_send_routing_info_request_t));
					//map_memzero(p_send_routing_info_req_temp, sizeof (map_send_routing_info_request_t));


					#if 0
					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_send_routing_info_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
					#endif
						corr_id = app_map_get_new_correlation_id();
						p_send_routing_info_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					//}

					memcpy(p_send_routing_info_req_temp,p_send_routing_info_req,sizeof (map_send_routing_info_request_t));

					memcpy(send_api_sri_temp,send_api_sri,sizeof(map_api_struct_t));

					p_send_routing_info_req_temp->header.invoke_id = message_counter + 1;

					//if(message_counter == (num_of_messages -1))
					p_send_routing_info_req_temp->header.last_component = 1;

					send_api_sri_temp->p_data = p_send_routing_info_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_sri_temp, &error);

					sri_request_counters++;

					if((sri_request_counters % 50000) == 0)
						print_sri_sm_lu_report();

				}

				usleep(delay);

			}

			#if 0

			app_mem_free(send_api);
			app_mem_free(p_send_routing_info_req);

			#endif

	}
        else if(!strncmp(api_name,"MAP_ROUTING_INFO_FOR_SM",strlen("MAP_ROUTING_INFO_FOR_SM"))) 
        {

			index = 0;


			if(send_api_srism == NULL)
			{
				send_api_srism = app_mem_get(sizeof(map_api_struct_t));
				map_memzero(send_api_srism,sizeof(map_api_struct_t));

				sprintf(filename,"../buffers/%s",api_name);
				fp_apis = fopen(filename,"r");
			}

			if(p_send_routing_info_sm_req == NULL)
			{
				
				p_send_routing_info_sm_req = (map_routing_info_for_sm_request_t *)\
							     app_mem_get (sizeof (map_routing_info_for_sm_request_t));
				map_memzero(p_send_routing_info_sm_req, sizeof (map_routing_info_for_sm_request_t));

				fill_api_header(&(send_api_srism->header), msc_user_id);
				send_api_srism->header.api_id = MAP_SEND_ROUTING_INFO_FOR_SM_REQUEST;
				send_api_srism->header.spare1 = g_sap;
				send_api_srism->header.ver = 3;
				send_api_srism->header.len = sizeof(map_routing_info_for_sm_request_t);

				fill_header(&p_send_routing_info_sm_req->header,corr_id);


				fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
				memset(bitmap_array,'\0',20);
				strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_ROUTING_INFO_FOR_SM  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
					if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");


						token_val_temp = (char *)malloc(strlen(token_val));
						strncpy(token_val_temp,token_val,strlen(token_val));

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
								p_send_routing_info_sm_req->arg.msisdn.length = atoi(token_length);
								p_send_routing_info_sm_req->arg.msisdn.value[counter] = hstoi(token_generic);
							}
							else if(index == 1)
								p_send_routing_info_sm_req->arg.sm_rp_pri = hstoi(token_generic);
							else if(index == 2)
							{
								p_send_routing_info_sm_req->arg.service_centre_address.length = atoi(token_length);
								p_send_routing_info_sm_req->arg.service_centre_address.value[counter] = hstoi(token_generic);
							}

							else if(index == 3)
							{

								p_send_routing_info_sm_req->arg.is_extension = atoi(token_generic);
							}

							else if(index == 4)
							{
								p_send_routing_info_sm_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter <p_send_routing_info_sm_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_send_routing_info_sm_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_send_routing_info_sm_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}


								}

							}
							else if(index == 5)
							{

								for(ext_counter = 0; ext_counter < p_send_routing_info_sm_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
									}		
									else{
										token_ext = strtok(NULL,":");
									}


									if(!strncmp(token_ext,"NULL",strlen("NULL")))
										p_send_routing_info_sm_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
									else
									{	
										p_send_routing_info_sm_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
													p_send_routing_info_sm_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


												}
												else
												{
													p_send_routing_info_sm_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
												}
											}

											finished_flag++;

										}
									}

								}
							}

							else if(index == 6)
								p_send_routing_info_sm_req->arg.is_gprs_support_indicator = atoi(token_length);
							else if(index == 7)
								p_send_routing_info_sm_req->arg.gprs_support_indicator= hstoi(token_length);

							else if(index == 8)
								p_send_routing_info_sm_req->arg.is_sm_rp_mti = atoi(token_generic);
							else if(index == 9)
								p_send_routing_info_sm_req->arg.sm_rp_mti = hstoi(token_generic); 		
							else if(index == 10)

								p_send_routing_info_sm_req->arg.is_sm_rp_smea = atoi(token_generic);
							else if(index == 11)
							{
								p_send_routing_info_sm_req->arg.sm_rp_smea.length =atoi(token_length);

								p_send_routing_info_sm_req->arg.sm_rp_smea.value[counter]=hstoi(token_generic);
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

				fclose(fp_apis);
			}


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{



				for(message_counter = 0; message_counter < no_mess_in_burst;message_counter++ )
				{


					if(seq_control == 15)
						seq_control = 1;
					else
						seq_control++;

					//printf("Sending The message \n");
					send_api_srism_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					//map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_send_routing_info_sm_req_temp = (map_routing_info_for_sm_request_t *)\
					app_mem_get(sizeof (map_routing_info_for_sm_request_t));
					//map_memzero(p_send_routing_info_sm_req_temp, sizeof (map_routing_info_for_sm_request_t));



					#if 0
					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_send_routing_info_sm_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
					#endif
						corr_id = app_map_get_new_correlation_id();
						p_send_routing_info_sm_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					//}

					memcpy(p_send_routing_info_sm_req_temp,p_send_routing_info_sm_req,sizeof (map_routing_info_for_sm_request_t));

					memcpy(send_api_srism_temp,send_api_srism,sizeof(map_api_struct_t));

					p_send_routing_info_sm_req_temp->header.invoke_id = message_counter + 1;

			//		if(message_counter == (num_of_messages -1))
						p_send_routing_info_sm_req_temp->header.last_component = 1;

					send_api_srism_temp->p_data = p_send_routing_info_sm_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_srism_temp, &error);


					srism_request_counters++;

					if((srism_request_counters%50000) == 0)
						print_srism();
				}

				usleep(delay);

			}


			#if 0
			app_mem_free(send_api_srism);
			app_mem_free(p_send_routing_info_sm_req);

			#endif

        }
        else if(!strncmp(api_name,"MAP_V2_ROUTING_INFO_FOR_SM",strlen("MAP_V2_ROUTING_INFO_FOR_SM"))) 
        {


			index = 0;

			map_v2_routing_info_for_sm_request_t *p_send_routing_info_req = NULL,*p_send_routing_info_req_temp = NULL;
			p_send_routing_info_req = (map_v2_routing_info_for_sm_request_t *)\
			app_mem_get (sizeof (map_v2_routing_info_for_sm_request_t));
			map_memzero(p_send_routing_info_req, sizeof (map_v2_routing_info_for_sm_request_t));

			fill_api_header(&(send_api->header), msc_user_id);
			send_api->header.api_id = MAP_SEND_ROUTING_INFO_FOR_SM_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 2;
			send_api->header.len = sizeof(map_v2_routing_info_for_sm_request_t);

			fill_header(&p_send_routing_info_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_ROUTING_INFO_FOR_SM  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");


                        token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
								p_send_routing_info_req->arg.msisdn.length = atoi(token_length);
								p_send_routing_info_req->arg.msisdn.value[counter] = hstoi(token_generic);
							}
							else if(index == 1)
								p_send_routing_info_req->arg.sm_rp_pri = hstoi(token_generic);
							else if(index == 2)
							{
								p_send_routing_info_req->arg.service_centre_address.length = atoi(token_length);
								p_send_routing_info_req->arg.service_centre_address.value[counter] = hstoi(token_generic);
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_send_routing_info_req_temp = (map_v2_routing_info_for_sm_request_t *)\
					app_mem_get(sizeof (map_v2_routing_info_for_sm_request_t));
					map_memzero(p_send_routing_info_req_temp, sizeof (map_v2_routing_info_for_sm_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_send_routing_info_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_send_routing_info_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_send_routing_info_req_temp,p_send_routing_info_req,sizeof (map_v2_routing_info_for_sm_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_send_routing_info_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
						p_send_routing_info_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_send_routing_info_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_send_routing_info_req);


        }

        else if(!strncmp(api_name,"MAP_V1_ROUTING_INFO_FOR_SM",strlen("MAP_V1_ROUTING_INFO_FOR_SM"))) 
        {


			index = 0;

			map_v1_send_routing_info_for_sm_request_t *p_send_routing_info_req = NULL,*p_send_routing_info_req_temp = NULL;
			p_send_routing_info_req = (map_v1_send_routing_info_for_sm_request_t *)\
			app_mem_get (sizeof (map_v1_send_routing_info_for_sm_request_t));
			map_memzero(p_send_routing_info_req, sizeof (map_v1_send_routing_info_for_sm_request_t));

		        send_api= app_mem_get(sizeof(map_api_struct_t));
                	map_memzero(send_api,sizeof(map_api_struct_t));

			fill_api_header(&(send_api->header), msc_user_id);
			send_api->header.api_id = MAP_SEND_ROUTING_INFO_FOR_SM_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 1;
			send_api->header.len = sizeof(map_v1_send_routing_info_for_sm_request_t);

			sprintf(filename,"../buffers/%s",api_name);
			fp_apis = fopen(filename,"r");


			fill_header(&p_send_routing_info_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_V1_ROUTING_INFO_FOR_SM is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");


                        token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
								p_send_routing_info_req->arg.ms_isdn.length = atoi(token_length);
								p_send_routing_info_req->arg.ms_isdn.value[counter] = hstoi(token_generic);
							}
							else if(index == 1)
								p_send_routing_info_req->arg.sm_rp_pri = hstoi(token_generic);
							else if(index == 2)
							{
								p_send_routing_info_req->arg.service_centre_address.length = atoi(token_length);
								p_send_routing_info_req->arg.service_centre_address.value[counter] = hstoi(token_generic);
							}
                            else if(index == 3)
							{
								p_send_routing_info_req->arg.is_cug_interlock =  atoi(token_generic);
							}
                            else if(index == 4)
							{
								p_send_routing_info_req->arg.cug_interlock.length = atoi(token_length);
								p_send_routing_info_req->arg.cug_interlock.value[counter] = hstoi(token_generic);
							}
                            else if(index == 5)
							{
								p_send_routing_info_req->arg.is_teleservice_code =  atoi(token_generic);
							}
                            else if(index == 6)
							{
								p_send_routing_info_req->arg.teleservice_code.length = atoi(token_length);
								p_send_routing_info_req->arg.teleservice_code.value[counter] = hstoi(token_generic);
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_send_routing_info_req_temp = (map_v1_send_routing_info_for_sm_request_t *)\
					app_mem_get(sizeof (map_v1_send_routing_info_for_sm_request_t));
					map_memzero(p_send_routing_info_req_temp, sizeof (map_v1_send_routing_info_for_sm_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_send_routing_info_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_send_routing_info_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_send_routing_info_req_temp,p_send_routing_info_req,sizeof (map_v1_send_routing_info_for_sm_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_send_routing_info_req_temp->header.invoke_id = message_counter + 1;

					//if(message_counter == (num_of_messages -1))
						p_send_routing_info_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_send_routing_info_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);
				sleep(5);

				}


			}

			app_mem_free(send_api);
			app_mem_free(p_send_routing_info_req);


        }
#if 0
		else if(!strncmp(api_name,"MAP_V1_CHECK_IMEI_REQUEST",strlen("MAP_V1_CHECK_IMEI_REQUEST"))) 
		{

			index = 0;

			
			map_v1_check_imei_request_t *p_v1_check_imei_req = NULL,*p_v1_check_imei_req_temp = NULL;
			p_v1_check_imei_req = (map_v1_check_imei_request_t *)\
			app_mem_get (sizeof (map_v1_check_imei_request_t));
			map_memzero(p_v1_check_imei_req, sizeof (map_v1_check_imei_request_t));

			fill_api_header(&(send_api->header), msc_user_id);
			send_api->header.api_id = MAP_CHECK_IMEI_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 1;
			send_api->header.len = sizeof(map_v1_check_imei_request_t);

			fill_header(&p_v1_check_imei_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_V1_CHECK_IMEI_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
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
								p_v1_check_imei_req->arg.imei.length = atoi(token_length);
								p_v1_check_imei_req->arg.imei.value[counter] = hstoi(token_generic);		
							}
							else if(index == 1)
								p_v1_check_imei_req->arg.is_imsi = atoi(token_generic);
							else if(index == 2)
							{
								p_v1_check_imei_req->arg.imsi.length = atoi(token_length);
								p_v1_check_imei_req->arg.imsi.value[counter] = hstoi(token_generic);
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v1_check_imei_req_temp = (map_v1_check_imei_request_t *)\
					app_mem_get(sizeof (map_v1_check_imei_request_t));
					map_memzero(p_v1_check_imei_req_temp, sizeof (map_v1_check_imei_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v1_check_imei_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v1_check_imei_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v1_check_imei_req_temp,p_v1_check_imei_req,sizeof (map_v1_check_imei_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v1_check_imei_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
						p_v1_check_imei_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_v1_check_imei_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_v1_check_imei_req);


		}

#endif
		else if(!strncmp(api_name,"MAP_V2_CHECK_IMEI_REQUEST",strlen("MAP_V2_CHECK_IMEI_REQUEST"))) 
		{

			index = 0;

			
			map_v2_check_imei_request_t *p_v2_check_imei_req = NULL,*p_v2_check_imei_req_temp = NULL;
			p_v2_check_imei_req = (map_v2_check_imei_request_t *)\
			app_mem_get (sizeof (map_v2_check_imei_request_t));
			map_memzero(p_v2_check_imei_req, sizeof (map_v2_check_imei_request_t));


			fill_api_header(&(send_api->header), msc_user_id);
			send_api->header.api_id = MAP_CHECK_IMEI_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 2;
			send_api->header.len = sizeof(map_v2_check_imei_request_t);

			fill_header(&p_v2_check_imei_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_V2_CHECK_IMEI_REQUEST  is successfully Parsed\n\n");
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
								p_v2_check_imei_req->arg.length = atoi(token_length);
								p_v2_check_imei_req->arg.value[counter] = hstoi(token_generic);		
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


			//for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
            for(;;)
			{
				//for(message_counter = 0; message_counter < num_of_messages;message_counter++ )

				if(delay >= 1000000)
				{
					tout.tv_sec = delay/1000000;
				}
				else
				{
					tout.tv_sec =0;
				}

				tout.tv_usec = delay - tout.tv_sec*1000000;


				select(0,NULL,NULL,NULL,&tout);

				for(message_counter = 0; message_counter < no_mess_in_burst;message_counter++ )
				{


					if(seq_control == 15)
						seq_control = 1;
					else
						seq_control++;

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v2_check_imei_req_temp = (map_v2_check_imei_request_t *)\
					app_mem_get(sizeof (map_v2_check_imei_request_t));
					map_memzero(p_v2_check_imei_req_temp, sizeof (map_v2_check_imei_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v2_check_imei_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v2_check_imei_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v2_check_imei_req_temp,p_v2_check_imei_req,sizeof (map_v2_check_imei_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v2_check_imei_req_temp->header.invoke_id = message_counter + 1;

					//if(message_counter == (num_of_messages -1))
						p_v2_check_imei_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_v2_check_imei_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);


				}

                //sleep(delay);

			}

			app_mem_free(send_api);
			app_mem_free(p_v2_check_imei_req);


		}
		else if(!strncmp(api_name,"MAP_CHECK_IMEI_REQUEST",strlen("MAP_CHECK_IMEI_REQUEST"))) 
		{

			index = 0;

			
			map_check_imei_request_t *p_check_imei_req = NULL,*p_check_imei_req_temp = NULL;
			p_check_imei_req = (map_check_imei_request_t *)\
			app_mem_get (sizeof (map_check_imei_request_t));
			map_memzero(p_check_imei_req, sizeof (map_check_imei_request_t));

			fill_api_header(&(send_api->header),msc_user_id);
			send_api->header.api_id = MAP_CHECK_IMEI_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 3;
			send_api->header.len = sizeof(map_check_imei_request_t);

			fill_header(&p_check_imei_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_ALERT_SERVICE_CENTRE_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

                        token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
								p_check_imei_req->arg.imei.length = atoi(token_length);
								p_check_imei_req->arg.imei.value[counter] = hstoi(token_generic);		
							}
							else if(index == 1)
							{
								p_check_imei_req->arg.requested_equipment_info.length = atoi(token_length);
								p_check_imei_req->arg.requested_equipment_info.value[counter] = hstoi(token_generic);
							}
							else if(index == 2)
								p_check_imei_req->arg.is_extension = atoi(token_generic);
							else if(index == 3)
							{
								p_check_imei_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter <p_check_imei_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_check_imei_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_check_imei_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 4)
							{
								
							  for(ext_counter = 0; ext_counter < p_check_imei_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_check_imei_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_check_imei_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_check_imei_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_check_imei_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{


                    if(seq_control == 15)
                         seq_control = 1;
                    else
                         seq_control++;

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_check_imei_req_temp = (map_check_imei_request_t *)\
					app_mem_get(sizeof (map_check_imei_request_t));
					map_memzero(p_check_imei_req_temp, sizeof (map_check_imei_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_check_imei_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_check_imei_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_check_imei_req_temp,p_check_imei_req,sizeof (map_check_imei_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_check_imei_req_temp->header.invoke_id = message_counter + 1;

					//if(message_counter == (num_of_messages -1)) For testing
						p_check_imei_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_check_imei_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

                    sleep(1);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_check_imei_req);


		}
		else if(!strncmp(api_name,"MAP_V1_PROCESS_UNSTRUCTURED_SS_DATA_REQUEST",strlen("MAP_V1_PROCESS_UNSTRUCTURED_SS_DATA_REQUEST"))) 
		{

			index = 0;

            send_api = app_mem_get(sizeof(map_api_struct_t));
            map_memzero(send_api,sizeof(map_api_struct_t));
			
			map_v1_process_unstructured_ss_data_request_t *p_v1_process_unstructured_ss_data_req = NULL,*p_v1_process_unstructured_ss_data_req_temp = NULL;
			p_v1_process_unstructured_ss_data_req = (map_v1_process_unstructured_ss_data_request_t *)\
			app_mem_get (sizeof (map_v1_process_unstructured_ss_data_request_t));
			map_memzero(p_v1_process_unstructured_ss_data_req, sizeof (map_v1_process_unstructured_ss_data_request_t));

			fill_api_header(&(send_api->header), hlr_user_id);
			send_api->header.api_id = MAP_PROCESS_UNSTRUCTURED_SS_DATA_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 1;
			send_api->header.len = sizeof(map_v1_process_unstructured_ss_data_request_t);

			fill_header(&p_v1_process_unstructured_ss_data_req->header,corr_id);

			sprintf(filename,"../buffers/%s",api_name);
			fp_apis = fopen(filename,"r");


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_ALERT_SERVICE_CENTRE_REQUEST  is successfully Parsed\n\n");
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
								p_v1_process_unstructured_ss_data_req->arg[counter] = hstoi(token_generic);


						}

						index++;

					}
					else{

						printf("No Optional Parameter is there so Please check\n");
						break;
					}

				}

			}


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < no_mess_in_burst;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v1_process_unstructured_ss_data_req_temp = (map_v1_process_unstructured_ss_data_request_t *)\
					app_mem_get(sizeof (map_v1_process_unstructured_ss_data_request_t));
					map_memzero(p_v1_process_unstructured_ss_data_req_temp, sizeof (map_v1_process_unstructured_ss_data_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v1_process_unstructured_ss_data_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v1_process_unstructured_ss_data_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v1_process_unstructured_ss_data_req_temp,p_v1_process_unstructured_ss_data_req,sizeof (map_v1_process_unstructured_ss_data_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v1_process_unstructured_ss_data_req_temp->header.invoke_id = message_counter + 1;

					//if(message_counter == (num_of_messages -1))
					p_v1_process_unstructured_ss_data_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_v1_process_unstructured_ss_data_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

                usleep(delay);

			}

			app_mem_free(send_api);
			app_mem_free(p_v1_process_unstructured_ss_data_req);


		}
		else if(!strncmp(api_name,"MAP_V2_PROCESS_UNSTRUCTURED_SS_DATA_REQUEST",strlen("MAP_V2_PROCESS_UNSTRUCTURED_SS_DATA_REQUEST"))) 
		{

			index = 0;

			send_api = app_mem_get(sizeof(map_api_struct_t));
            map_memzero(send_api,sizeof(map_api_struct_t));

			map_v2_process_unstructured_ss_request_request_t *p_v2_process_unstructured_ss_data_req = NULL,*p_v2_process_unstructured_ss_data_req_temp = NULL;
			p_v2_process_unstructured_ss_data_req = (map_v2_process_unstructured_ss_request_request_t *)\
			app_mem_get (sizeof (map_v2_process_unstructured_ss_request_request_t));
			map_memzero(p_v2_process_unstructured_ss_data_req, sizeof (map_v2_process_unstructured_ss_request_request_t));

			fill_api_header(&(send_api->header), vlr_user_id);
			send_api->header.api_id = MAP_PROCESS_UNSTRUCTURED_SS_REQUEST_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 2;
			send_api->header.len = sizeof(map_v2_process_unstructured_ss_request_request_t);



			fill_header(&p_v2_process_unstructured_ss_data_req->header,corr_id);

			sprintf(filename,"../buffers/%s",api_name);
			fp_apis = fopen(filename,"r");

			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf(" MAP_PROCESS_UNSTRUCTURED_SS_REQUEST_REQUEST is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1') for testing only
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
								p_v2_process_unstructured_ss_data_req->arg.ussd_data_coding_scheme.length = atoi(token_length);
								p_v2_process_unstructured_ss_data_req->arg.ussd_data_coding_scheme.value[counter] = hstoi(token_generic);
							}
							else if(index == 1)
							{

								p_v2_process_unstructured_ss_data_req->arg.ussd_string.length = atoi(token_length);
								p_v2_process_unstructured_ss_data_req->arg.ussd_string.value[counter] = hstoi(token_generic);

							}	
							else if(index == 2)
								p_v2_process_unstructured_ss_data_req->arg.is_alerting_pattern = atoi(token_generic);
							else if(index == 3)
							{
								p_v2_process_unstructured_ss_data_req->arg.alerting_pattern.length =atoi(token_length); 
								p_v2_process_unstructured_ss_data_req->arg.alerting_pattern.value[counter] =  hstoi(token_generic);
							}
							else if(index == 4)
							p_v2_process_unstructured_ss_data_req->arg.is_msisdn = atoi(token_generic);

							else if(index == 5)
							{
								p_v2_process_unstructured_ss_data_req->arg.msisdn.length =atoi(token_length); 
								p_v2_process_unstructured_ss_data_req->arg.msisdn.value[counter]=hstoi(token_generic);
							}
						}

						index++;

					}
                    #if 0    
					else{

						printf("No Optional Parameter is there so Please check\n");
						break;
					}
                    #endif

				}

			}


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < no_mess_in_burst;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v2_process_unstructured_ss_data_req_temp = (map_v2_process_unstructured_ss_request_request_t *)\
					app_mem_get(sizeof (map_v2_process_unstructured_ss_request_request_t));
					map_memzero(p_v2_process_unstructured_ss_data_req_temp, sizeof (map_v2_process_unstructured_ss_request_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v2_process_unstructured_ss_data_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v2_process_unstructured_ss_data_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v2_process_unstructured_ss_data_req_temp,p_v2_process_unstructured_ss_data_req,sizeof (map_v2_process_unstructured_ss_request_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v2_process_unstructured_ss_data_req_temp->header.invoke_id = message_counter + 1;

					//if(message_counter == (num_of_messages -1))
						p_v2_process_unstructured_ss_data_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_v2_process_unstructured_ss_data_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

                usleep(delay);

			}

			app_mem_free(send_api);
			app_mem_free(p_v2_process_unstructured_ss_data_req);


		}
		else if(!strncmp(api_name,"MAP_V2_UNSTRUCTURED_SS_REQUEST_REQUEST",strlen("MAP_V2_UNSTRUCTURED_SS_REQUEST_REQUEST"))) 
		{

			index = 0;

			
			//map_v2_unstructured_ss_request_request_t *p_v2_unstructured_ss_data_req = NULL,*p_v2_unstructured_ss_data_req_temp = NULL;
			if(send_api_ussr == NULL)
            {
                send_api_ussr = app_mem_get(sizeof(map_api_struct_t));
                map_memzero(send_api_ussr,sizeof(map_api_struct_t));

                sprintf(filename,"../buffers/%s",api_name);
                fp_apis = fopen(filename,"r");
            }

			
			if(p_v2_unstructured_ss_data_req == NULL)
			{
			p_v2_unstructured_ss_data_req = (map_v2_unstructured_ss_request_request_t *)\
			app_mem_get (sizeof (map_v2_unstructured_ss_request_request_t));
			map_memzero(p_v2_unstructured_ss_data_req, sizeof (map_v2_unstructured_ss_request_request_t));

			fill_api_header(&(send_api_ussr->header), vlr_user_id);
			send_api_ussr->header.api_id = MAP_UNSTRUCTURED_SS_REQUEST_REQUEST;
			send_api_ussr->header.spare1 = g_sap;
			send_api_ussr->header.ver = 2;
			send_api_ussr->header.len = sizeof(map_v2_unstructured_ss_request_request_t);

			fill_header(&p_v2_unstructured_ss_data_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_ALERT_SERVICE_CENTRE_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    			if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

						//printf("Token Length [%s] \n",token_length);
						//printf("Token Value [%s] \n",token_val);
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
								p_v2_unstructured_ss_data_req->arg.ussd_data_coding_scheme.length = atoi(token_length);
								p_v2_unstructured_ss_data_req->arg.ussd_data_coding_scheme.value[counter] = hstoi(token_generic);
							}
							else if(index == 1)
							{

								p_v2_unstructured_ss_data_req->arg.ussd_string.length = atoi(token_length);
								p_v2_unstructured_ss_data_req->arg.ussd_string.value[counter] = hstoi(token_generic);

							}	
							else if(index == 2)
								p_v2_unstructured_ss_data_req->arg.is_alerting_pattern = atoi(token_generic);
							else if(index == 3)
							{
								p_v2_unstructured_ss_data_req->arg.alerting_pattern.length =atoi(token_length); 
								p_v2_unstructured_ss_data_req->arg.alerting_pattern.value[counter]=hstoi(token_generic);
							}
							else if(index == 4)
							p_v2_unstructured_ss_data_req->arg.is_msisdn = atoi(token_generic);

							else if(index == 5)
							{
								p_v2_unstructured_ss_data_req->arg.msisdn.length =atoi(token_length); 
								p_v2_unstructured_ss_data_req->arg.msisdn.value[counter]=hstoi(token_generic);
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

			expiry_time.tv_sec = 0;	
			expiry_time.tv_usec = 0;


			//printf("number of messages configured: [%d]\n", no_mess_in_burst) ;


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			//for(;;)
			{

				#if 0

				if(delay >= 1000000)
				{
					tout.tv_sec = delay/1000000;
				}
				else
				{
					tout.tv_sec =0;
				}

				tout.tv_usec = delay - tout.tv_sec*1000000;

				select(0,NULL,NULL,NULL,&tout);
				#endif


				#if 0
				if (gettimeofday(&current_time,NULL) == 0)
				{
					if (current_time.tv_sec > expiry_time.tv_sec || ( current_time.tv_sec == expiry_time.tv_sec && current_time.tv_usec >= expiry_time.tv_usec))
					{


						expiry_time.tv_sec =    current_time.tv_sec
							+ (current_time.tv_usec + delay)/1000000;
						expiry_time.tv_usec =   (current_time.tv_usec + delay)%1000000;

				#endif

						for(message_counter = 0; message_counter < no_mess_in_burst;message_counter++ )
						{

							if(seq_control == 15)
								seq_control = 1;
							else
								seq_control++;

							//printf("Sending The message \n");
							send_api_ussr_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
							map_memzero(send_api_ussr_temp,sizeof(map_api_struct_t));

							p_v2_unstructured_ss_data_req_temp = (map_v2_unstructured_ss_request_request_t *)\
											     app_mem_get(sizeof (map_v2_unstructured_ss_request_request_t));
							map_memzero(p_v2_unstructured_ss_data_req_temp, sizeof (map_v2_unstructured_ss_request_request_t));


							#if 0

							if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
							{
								if(message_counter == 0)
								{
									corr_id = app_map_get_new_correlation_id();
									p_v2_unstructured_ss_data_req->header.corr_id =  corr_id;

									send_open_req(api_name ,corr_id);
								}
							}
							else{
							#endif

				
								if(corr_id ==0 )
								{
									corr_id = app_map_get_new_correlation_id();  //Send Open Request Only once
									send_open_req(api_name ,corr_id);
									p_v2_unstructured_ss_data_req_temp->header.last_component = 0;
								}
								else
									p_v2_unstructured_ss_data_req_temp->header.last_component = 1;
								
								printf("Correlation Id is ###################[%d]\n\n",corr_id);

								p_v2_unstructured_ss_data_req->header.corr_id =  corr_id;
						//	}

							memcpy(p_v2_unstructured_ss_data_req_temp,p_v2_unstructured_ss_data_req,sizeof (map_v2_unstructured_ss_request_request_t));

							memcpy(send_api_ussr_temp,send_api_ussr,sizeof(map_api_struct_t));

							p_v2_unstructured_ss_data_req_temp->header.invoke_id = ++tmp_var;



							

							printf("Last Component is ############[%d] \n",p_v2_unstructured_ss_data_req_temp->header.last_component);
							send_api_ussr_temp->p_data = p_v2_unstructured_ss_data_req_temp;
							app_map_send_to_app_map((unsigned char *)send_api_ussr_temp, &error);

							//if(p_v2_unstructured_ss_data_req_temp->header.last_component == 1)

							/*Changed for Testing
							//if(message_counter == (num_of_messages -1))
							p_v2_unstructured_ss_data_req_temp->header.last_component = 1;

							send_api_ussr_temp->p_data = p_v2_unstructured_ss_data_req_temp;
							app_map_send_to_app_map((unsigned char *)send_api_ussr_temp, &error);

							*/

							ussd_request_counters++;

                            if((ussd_request_counters%50000) == 0)
                                print_ussd_report();

						}

						usleep(delay);

					#if 0
					}
					else
					{
					//	printf("I m sleeping for [%d]",(((expiry_time.tv_sec*1000000) + expiry_time.tv_usec)  - (( current_time.tv_sec*1000000) + current_time.tv_usec))/1001);
						usleep(((expiry_time.tv_sec*1000000) + expiry_time.tv_usec)  - (( current_time.tv_sec*1000000) + current_time.tv_usec));
					}

				}
				#endif

				//	printf("messages sent : [%lu]\n", ussd_request_counters);

				//if((ussd_request_counters%50000) == 0)
				//	print_ussd_report();

			}
                    					//	send_delimiter_request(p_v2_unstructured_ss_data_req->header.corr_id);

			#if 0
			app_mem_free(send_api);
			app_mem_free(p_v2_unstructured_ss_data_req);

			#endif


		}
		else if(!strncmp(api_name,"MAP_V2_UNSTRUCTURED_SS_NOTIFY_REQUEST",strlen("MAP_V2_UNSTRUCTURED_SS_NOTIFY_REQUEST"))) 
		{

			index = 0;

			
			map_v2_unstructured_ss_notify_request_t *p_v2_unstructured_ss_notify_req = NULL,*p_v2_unstructured_ss_notify_req_temp = NULL;
			p_v2_unstructured_ss_notify_req = (map_v2_unstructured_ss_notify_request_t *)\
			app_mem_get (sizeof (map_v2_unstructured_ss_notify_request_t));
			map_memzero(p_v2_unstructured_ss_notify_req, sizeof (map_v2_unstructured_ss_notify_request_t));

			fill_api_header(&(send_api->header), vlr_user_id);
			send_api->header.api_id = MAP_UNSTRUCTURED_SS_NOTIFY_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 2;
			send_api->header.len = sizeof(map_v2_unstructured_ss_notify_request_t);

			fill_header(&p_v2_unstructured_ss_notify_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_V2_UNSTRUCTURED_SS_NOTIFY_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
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
								p_v2_unstructured_ss_notify_req->arg.ussd_data_coding_scheme.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.ussd_data_coding_scheme.value[counter] = hstoi(token_generic);
							}
							else if(index == 1)
							{

								p_v2_unstructured_ss_notify_req->arg.ussd_string.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.ussd_string.value[counter] = hstoi(token_generic);

							}	
							else if(index == 2)
								p_v2_unstructured_ss_notify_req->arg.is_alerting_pattern = atoi(token_generic);
							else if(index == 3)
							{
								p_v2_unstructured_ss_notify_req->arg.alerting_pattern.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.alerting_pattern.value[counter] =hstoi(token_generic);
							}
							else if(index == 4)
							p_v2_unstructured_ss_notify_req->arg.is_msisdn = atoi(token_generic);

							else if(index == 5)
							{
								p_v2_unstructured_ss_notify_req->arg.msisdn.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.msisdn.value[counter] =hstoi(token_generic);
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp = (map_v2_unstructured_ss_notify_request_t *)\
					app_mem_get(sizeof (map_v2_unstructured_ss_notify_request_t));
					map_memzero(p_v2_unstructured_ss_notify_req_temp, sizeof (map_v2_unstructured_ss_notify_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v2_unstructured_ss_notify_req_temp,p_v2_unstructured_ss_notify_req,sizeof (map_v2_unstructured_ss_notify_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
						p_v2_unstructured_ss_notify_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_v2_unstructured_ss_notify_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_v2_unstructured_ss_notify_req);


		}

        else if(!strncmp(api_name,"MAP_PROVIDE_ROAMING_NUMBER_REQUEST",strlen("MAP_PROVIDE_ROAMING_NUMBER_REQUEST"))) 
		{

			index = 0;

			
			map_provide_roaming_number_request_t *p_v2_unstructured_ss_notify_req = NULL,*p_v2_unstructured_ss_notify_req_temp = NULL;
			p_v2_unstructured_ss_notify_req = (map_provide_roaming_number_request_t *)\
			app_mem_get (sizeof (map_provide_roaming_number_request_t));
			map_memzero(p_v2_unstructured_ss_notify_req, sizeof (map_provide_roaming_number_request_t));

			fill_api_header(&(send_api->header), hlr_user_id);
			send_api->header.api_id = MAP_PROVIDE_ROAMING_NUMBER_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 3;
			send_api->header.len = sizeof(map_provide_roaming_number_request_t);

			fill_header(&p_v2_unstructured_ss_notify_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_PROVIDE_ROAMING_NUMBER_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

                        token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
								p_v2_unstructured_ss_notify_req->arg.imsi.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.imsi.value[counter] = hstoi(token_generic);
							}
							else if(index == 1)
							{

								p_v2_unstructured_ss_notify_req->arg.msc_number.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.msc_number.value[counter] = hstoi(token_generic);

							}	
							else if(index == 2)
								p_v2_unstructured_ss_notify_req->arg.is_msisdn = atoi(token_generic);
							else if(index == 3)
							{
								p_v2_unstructured_ss_notify_req->arg.msisdn.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.msisdn.value[counter] =hstoi(token_generic);
							}
                        	else if(index == 4)
								p_v2_unstructured_ss_notify_req->arg.is_lmsi = atoi(token_generic);

                            else if(index == 5)
							{
								p_v2_unstructured_ss_notify_req->arg.lmsi.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.lmsi.value[counter] =hstoi(token_generic);
							}
                            else if(index == 6)
								p_v2_unstructured_ss_notify_req->arg.is_gsm_bearer_capability = atoi(token_generic);
                            else if(index == 7)
                                p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.protocol_id = atoi(token_generic);

                           else if(index == 8)
							{
								p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.signal_info.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.signal_info.value[counter] =hstoi(token_generic);
							} 

                            else if(index == 9)
								p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.is_extension = atoi(token_generic);
							else if(index == 10)
							{
								p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter \
                                <p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 11)
							{
								
							  for(ext_counter = 0; ext_counter <  \
                              p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
							}

                           else if(index == 12)
                                p_v2_unstructured_ss_notify_req->arg.is_network_signal_info = atoi(token_generic); 
                           else if(index == 13)
                                p_v2_unstructured_ss_notify_req->arg.network_signal_info.protocol_id = atoi(token_generic); 
                           else if(index == 14)
							{
								p_v2_unstructured_ss_notify_req->arg.network_signal_info.signal_info.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.network_signal_info.signal_info.value[counter] =hstoi(token_generic);
							}
                            else if(index == 15)
								p_v2_unstructured_ss_notify_req->arg.network_signal_info.is_extension = atoi(token_generic);
							else if(index == 16)
							{
								p_v2_unstructured_ss_notify_req->arg.network_signal_info.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter \
                                <p_v2_unstructured_ss_notify_req->arg.network_signal_info.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_v2_unstructured_ss_notify_req->arg.network_signal_info.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.network_signal_info.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 17)
							{
								
							  for(ext_counter = 0; ext_counter <  \
                              p_v2_unstructured_ss_notify_req->arg.network_signal_info.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_v2_unstructured_ss_notify_req->arg.network_signal_info.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_v2_unstructured_ss_notify_req->arg.network_signal_info.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_v2_unstructured_ss_notify_req->arg.network_signal_info.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.network_signal_info.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
							}

                            else if(index == 18)
                                p_v2_unstructured_ss_notify_req->arg.is_suppression_of_announcement = atoi(token_generic); 
                           else if(index == 19)
							{
								p_v2_unstructured_ss_notify_req->arg.suppression_of_announcement =hstoi(token_generic);
							}
                            else if(index == 20)
                                p_v2_unstructured_ss_notify_req->arg.is_gmsc_address = atoi(token_generic); 
                           else if(index == 21)
							{
								p_v2_unstructured_ss_notify_req->arg.gmsc_address.length=atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.gmsc_address.value[counter]=hstoi(token_generic);

							}
                            else if(index == 22)
                                p_v2_unstructured_ss_notify_req->arg.is_call_reference_number = atoi(token_generic); 
                           else if(index == 23)
							{
								p_v2_unstructured_ss_notify_req->arg.call_reference_number.length=atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.call_reference_number.value[counter]=hstoi(token_generic);

							}
                            else if(index == 24)
                                p_v2_unstructured_ss_notify_req->arg.is_or_interrogation = atoi(token_generic); 
                           else if(index == 25)
							{
								p_v2_unstructured_ss_notify_req->arg.or_interrogation=hstoi(token_generic);

							}

                            else if(index == 26)
								p_v2_unstructured_ss_notify_req->arg.is_extension = atoi(token_generic);
							else if(index == 27)
							{
								p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter \
                                <p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 28)
							{
								
							  for(ext_counter = 0; ext_counter <  \
                              p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
							}

                            else if(index == 29)
                                p_v2_unstructured_ss_notify_req->arg.is_alerting_pattern = atoi(token_generic); 
                           else if(index == 30)
							{
								p_v2_unstructured_ss_notify_req->arg.alerting_pattern.length=atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.alerting_pattern.value[counter]=hstoi(token_generic);

							}
                            else if(index == 31)
                                p_v2_unstructured_ss_notify_req->arg.is_ccbs_call = atoi(token_generic); 
                           else if(index == 32)
							{
								p_v2_unstructured_ss_notify_req->arg.ccbs_call=hstoi(token_generic);

							}
                            else if(index == 33)
                                p_v2_unstructured_ss_notify_req->arg.is_supported_camel_phases_in_interrogating_node = atoi(token_generic); 
                           else if(index == 34)
							{
								p_v2_unstructured_ss_notify_req->arg.supported_camel_phases_in_interrogating_node.length=atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.supported_camel_phases_in_interrogating_node.value[counter]=hstoi(token_generic);

							}
                            else if(index == 35)
                                p_v2_unstructured_ss_notify_req->arg.is_additional_signal_info = atoi(token_generic); 
                           else if(index == 36)
							{
								p_v2_unstructured_ss_notify_req->arg.additional_signal_info.ext_protocol_id=atoi(token_generic);
							
							}

                            else if(index == 37)
							{
								p_v2_unstructured_ss_notify_req->arg.additional_signal_info.signal_info.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.additional_signal_info.signal_info.value[counter] =hstoi(token_generic);
							}
                            else if(index == 38)
								p_v2_unstructured_ss_notify_req->arg.additional_signal_info.is_extension = atoi(token_generic);
							else if(index == 39)
							{
								p_v2_unstructured_ss_notify_req->arg.additional_signal_info.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter \
                                <p_v2_unstructured_ss_notify_req->arg.additional_signal_info.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_v2_unstructured_ss_notify_req->arg.additional_signal_info.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.additional_signal_info.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 40)
							{
								
							  for(ext_counter = 0; ext_counter <  \
                              p_v2_unstructured_ss_notify_req->arg.additional_signal_info.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_v2_unstructured_ss_notify_req->arg.additional_signal_info.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_v2_unstructured_ss_notify_req->arg.additional_signal_info.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_v2_unstructured_ss_notify_req->arg.additional_signal_info.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.additional_signal_info.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
							}

                            else if(index == 41)
                                p_v2_unstructured_ss_notify_req->arg.is_or_not_supported_in_gmsc = atoi(token_generic); 
                           else if(index == 42)
							{
								p_v2_unstructured_ss_notify_req->arg.or_not_supported_in_gmsc=hstoi(token_generic);
							
							}
                           else if(index == 43)
                                p_v2_unstructured_ss_notify_req->arg.is_pre_paging_supported = atoi(token_generic); 
                           else if(index == 44)
							{
								p_v2_unstructured_ss_notify_req->arg.pre_paging_supported=hstoi(token_generic);
							
							}
                            else if(index == 45)
                                p_v2_unstructured_ss_notify_req->arg.is_long_ftn_supported = atoi(token_generic); 
                           else if(index == 46)
							{
								p_v2_unstructured_ss_notify_req->arg.long_ftn_supported=hstoi(token_generic);
							
							}
                            else if(index == 47)
                                p_v2_unstructured_ss_notify_req->arg.is_suppress_vt_csi = atoi(token_generic); 
                           else if(index == 48)
							{
								p_v2_unstructured_ss_notify_req->arg.suppress_vt_csi=hstoi(token_generic);
							
							}
                             else if(index == 49)
                                p_v2_unstructured_ss_notify_req->arg.is_offered_camel4_csis_in_interrogating_node = atoi(token_generic); 
                           else if(index == 50)
							{
								p_v2_unstructured_ss_notify_req->arg.offered_camel4_csis_in_interrogating_node.length=atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.offered_camel4_csis_in_interrogating_node.value[counter]=hstoi(token_generic);
							
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp = (map_provide_roaming_number_request_t *)\
					app_mem_get(sizeof (map_provide_roaming_number_request_t));
					map_memzero(p_v2_unstructured_ss_notify_req_temp, sizeof (map_provide_roaming_number_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v2_unstructured_ss_notify_req_temp,p_v2_unstructured_ss_notify_req,sizeof (map_provide_roaming_number_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
                    {
						p_v2_unstructured_ss_notify_req_temp->header.last_component = 1;
					//	p_v2_unstructured_ss_notify_req_temp->header.last_component = 0;
                    }

                    //p_v2_unstructured_ss_notify_req_temp->header.corr_id =global_corr_id; 
					send_api_temp->p_data = p_v2_unstructured_ss_notify_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

                    //send_delimiter_request(p_v2_unstructured_ss_notify_req_temp->header.corr_id);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_v2_unstructured_ss_notify_req);


		}

        else
        if(!strncmp(api_name,"MAP_V1_PROVIDE_ROAMING_NUMBER_REQUEST",strlen("MAP_V1_PROVIDE_ROAMING_NUMBER_REQUEST"))) 
		{

			index = 0;

			
			map_v1_provide_roaming_number_request_t *p_v2_unstructured_ss_notify_req = NULL,*p_v2_unstructured_ss_notify_req_temp = NULL;
			p_v2_unstructured_ss_notify_req = (map_v1_provide_roaming_number_request_t *)\
			app_mem_get (sizeof (map_v1_provide_roaming_number_request_t));
			map_memzero(p_v2_unstructured_ss_notify_req, sizeof (map_v1_provide_roaming_number_request_t));

			fill_api_header(&(send_api->header), hlr_user_id);
			send_api->header.api_id = MAP_PROVIDE_ROAMING_NUMBER_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 1;
			send_api->header.len = sizeof(map_v1_provide_roaming_number_request_t);

			fill_header(&p_v2_unstructured_ss_notify_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_V1_PROVIDE_ROAMING_NUMBER_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

                        token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
								p_v2_unstructured_ss_notify_req->arg.imsi.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.imsi.value[counter] = hstoi(token_generic);
							}
                            else if(index == 1)
                                p_v2_unstructured_ss_notify_req->arg.is_msc_number= atoi(token_generic);
							else if(index == 2)
							{

								p_v2_unstructured_ss_notify_req->arg.msc_number.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.msc_number.value[counter] = hstoi(token_generic);

							}	
							else if(index == 3)
								p_v2_unstructured_ss_notify_req->arg.is_ms_isdn = atoi(token_generic);
							else if(index == 4)
							{
								p_v2_unstructured_ss_notify_req->arg.ms_isdn.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.ms_isdn.value[counter] =hstoi(token_generic);
							}
                        	else if(index == 5)
								p_v2_unstructured_ss_notify_req->arg.is_previous_roaming_number = atoi(token_generic);

                            else if(index == 6)
							{
								p_v2_unstructured_ss_notify_req->arg.previous_roaming_number.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.previous_roaming_number.value[counter] =hstoi(token_generic);
							}
                        	else if(index == 7)
								p_v2_unstructured_ss_notify_req->arg.is_l_msld = atoi(token_generic);

                            else if(index == 8)
							{
								p_v2_unstructured_ss_notify_req->arg.l_msld.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.l_msld.value[counter] =hstoi(token_generic);
							}
                            else if(index == 9)
								p_v2_unstructured_ss_notify_req->arg.is_g_sm_bearer_capability = atoi(token_generic);
                            else if(index == 10)
                                p_v2_unstructured_ss_notify_req->arg.g_sm_bearer_capability.protocol_id = atoi(token_generic);

                           else if(index == 11)
							{
								p_v2_unstructured_ss_notify_req->arg.g_sm_bearer_capability.signal_info.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.g_sm_bearer_capability.signal_info.value[counter] =hstoi(token_generic);
							} 

                            else if(index == 12)
                                p_v2_unstructured_ss_notify_req->arg.is_network_signal_info = atoi(token_generic); 
                           else if(index == 13)
                                p_v2_unstructured_ss_notify_req->arg.network_signal_info.protocol_id = atoi(token_generic); 
                           else if(index == 14)
							{
								p_v2_unstructured_ss_notify_req->arg.network_signal_info.signal_info.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.network_signal_info.signal_info.value[counter] =hstoi(token_generic);
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp = (map_v1_provide_roaming_number_request_t *)\
					app_mem_get(sizeof (map_v1_provide_roaming_number_request_t));
					map_memzero(p_v2_unstructured_ss_notify_req_temp, sizeof (map_v1_provide_roaming_number_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v2_unstructured_ss_notify_req_temp,p_v2_unstructured_ss_notify_req,sizeof (map_v1_provide_roaming_number_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
                    {
						p_v2_unstructured_ss_notify_req_temp->header.last_component = 1;
					//	p_v2_unstructured_ss_notify_req_temp->header.last_component = 0;
                    }

                    //p_v2_unstructured_ss_notify_req_temp->header.corr_id =global_corr_id; 
					send_api_temp->p_data = p_v2_unstructured_ss_notify_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

                    //send_delimiter_request(p_v2_unstructured_ss_notify_req_temp->header.corr_id);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_v2_unstructured_ss_notify_req);


		}
        else
        if(!strncmp(api_name,"MAP_V2_PROVIDE_ROAMING_NUMBER_REQUEST",strlen("MAP_V2_PROVIDE_ROAMING_NUMBER_REQUEST"))) 
		{

			index = 0;

			
			map_v2_provide_roaming_number_request_t *p_v2_unstructured_ss_notify_req = NULL,*p_v2_unstructured_ss_notify_req_temp = NULL;
			p_v2_unstructured_ss_notify_req = (map_v2_provide_roaming_number_request_t *)\
			app_mem_get (sizeof (map_v2_provide_roaming_number_request_t));
			map_memzero(p_v2_unstructured_ss_notify_req, sizeof (map_v2_provide_roaming_number_request_t));

			fill_api_header(&(send_api->header), hlr_user_id);
			send_api->header.api_id = MAP_PROVIDE_ROAMING_NUMBER_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 2;
			send_api->header.len = sizeof(map_v2_provide_roaming_number_request_t);

			fill_header(&p_v2_unstructured_ss_notify_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_V2_PROVIDE_ROAMING_NUMBER_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

                        token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
								p_v2_unstructured_ss_notify_req->arg.imsi.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.imsi.value[counter] = hstoi(token_generic);
							}
                            else if(index == 1)
                                p_v2_unstructured_ss_notify_req->arg.is_msc_number= atoi(token_generic);
							else if(index == 2)
							{

								p_v2_unstructured_ss_notify_req->arg.msc_number.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.msc_number.value[counter] = hstoi(token_generic);

							}	
							else if(index == 3)
								p_v2_unstructured_ss_notify_req->arg.is_msisdn = atoi(token_generic);
							else if(index == 4)
							{
								p_v2_unstructured_ss_notify_req->arg.msisdn.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.msisdn.value[counter] =hstoi(token_generic);
							}
                        	else if(index == 5)
								p_v2_unstructured_ss_notify_req->arg.is_previous_roaming_number = atoi(token_generic);

                            else if(index == 6)
							{
								p_v2_unstructured_ss_notify_req->arg.previous_roaming_number.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.previous_roaming_number.value[counter] =hstoi(token_generic);
							}
                        	else if(index == 7)
								p_v2_unstructured_ss_notify_req->arg.is_lmsi = atoi(token_generic);

                            else if(index == 8)
							{
								p_v2_unstructured_ss_notify_req->arg.lmsi.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.lmsi.value[counter] =hstoi(token_generic);
							}
                            else if(index == 9)
								p_v2_unstructured_ss_notify_req->arg.is_gsm_bearer_capability = atoi(token_generic);
                            else if(index == 10)
                                p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.protocol_id = atoi(token_generic);

                           else if(index == 11)
							{
								p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.signal_info.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.gsm_bearer_capability.signal_info.value[counter] =hstoi(token_generic);
							} 

                            else if(index == 12)
                                p_v2_unstructured_ss_notify_req->arg.is_network_signal_info = atoi(token_generic); 
                           else if(index == 13)
                                p_v2_unstructured_ss_notify_req->arg.network_signal_info.protocol_id = atoi(token_generic); 
                           else if(index == 14)
							{
								p_v2_unstructured_ss_notify_req->arg.network_signal_info.signal_info.length =atoi(token_length); 
								p_v2_unstructured_ss_notify_req->arg.network_signal_info.signal_info.value[counter] =hstoi(token_generic);
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp = (map_v2_provide_roaming_number_request_t *)\
					app_mem_get(sizeof (map_v2_provide_roaming_number_request_t));
					map_memzero(p_v2_unstructured_ss_notify_req_temp, sizeof (map_v2_provide_roaming_number_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v2_unstructured_ss_notify_req_temp,p_v2_unstructured_ss_notify_req,sizeof (map_v2_provide_roaming_number_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
                    {
						p_v2_unstructured_ss_notify_req_temp->header.last_component = 1;
					//	p_v2_unstructured_ss_notify_req_temp->header.last_component = 0;
                    }

                    //p_v2_unstructured_ss_notify_req_temp->header.corr_id =global_corr_id; 
					send_api_temp->p_data = p_v2_unstructured_ss_notify_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

                    //send_delimiter_request(p_v2_unstructured_ss_notify_req_temp->header.corr_id);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_v2_unstructured_ss_notify_req);


		}

        else
        if(!strncmp(api_name,"MAP_SET_REPORTING_STATE_REQUEST",strlen("MAP_SET_REPORTING_STATE_REQUEST"))) 
		{

			index = 0;

			
			map_set_reporting_state_request_t *p_v2_unstructured_ss_notify_req = NULL,*p_v2_unstructured_ss_notify_req_temp = NULL;
			p_v2_unstructured_ss_notify_req = (map_set_reporting_state_request_t *)\
			app_mem_get (sizeof (map_set_reporting_state_request_t));
			map_memzero(p_v2_unstructured_ss_notify_req, sizeof (map_set_reporting_state_request_t));

			fill_api_header(&(send_api->header), hlr_user_id);
			send_api->header.api_id = MAP_SET_REPORTING_STATE_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 3;
			send_api->header.len = sizeof(map_set_reporting_state_request_t);

			fill_header(&p_v2_unstructured_ss_notify_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_SET_REPORTING_STATE_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

                        token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
                                p_v2_unstructured_ss_notify_req->arg.is_imsi =atoi(token_generic);
							if(index == 1)
							{
								p_v2_unstructured_ss_notify_req->arg.imsi.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.imsi.value[counter] = hstoi(token_generic);
							}
                            else if(index == 2)
                                p_v2_unstructured_ss_notify_req->arg.is_lmsi= atoi(token_generic);
							else if(index == 3)
							{

								p_v2_unstructured_ss_notify_req->arg.lmsi.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.lmsi.value[counter] = hstoi(token_generic);

							}	
							else if(index == 4)
								p_v2_unstructured_ss_notify_req->arg.is_ccbs_monitoring = atoi(token_generic);
							else if(index == 5)
							{
								p_v2_unstructured_ss_notify_req->arg.ccbs_monitoring=atoi(token_generic); 
							}

                            else if(index == 6)
								p_v2_unstructured_ss_notify_req->arg.is_extension = atoi(token_generic);
							else if(index == 7)
							{
								p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter \
                                <p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 8)
							{
								
							  for(ext_counter = 0; ext_counter <  \
                              p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp = (map_set_reporting_state_request_t *)\
					app_mem_get(sizeof (map_set_reporting_state_request_t));
					map_memzero(p_v2_unstructured_ss_notify_req_temp, sizeof (map_set_reporting_state_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v2_unstructured_ss_notify_req_temp,p_v2_unstructured_ss_notify_req,sizeof (map_set_reporting_state_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
                    {
						p_v2_unstructured_ss_notify_req_temp->header.last_component = 1;
					//	p_v2_unstructured_ss_notify_req_temp->header.last_component = 0;
                    }

                    //p_v2_unstructured_ss_notify_req_temp->header.corr_id =global_corr_id; 
					send_api_temp->p_data = p_v2_unstructured_ss_notify_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

                    //send_delimiter_request(p_v2_unstructured_ss_notify_req_temp->header.corr_id);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_v2_unstructured_ss_notify_req);


		}
        else
        if(!strncmp(api_name,"MAP_STATUS_REPORT_REQUEST",strlen("MAP_STATUS_REPORT_REQUEST"))) 
		{

			index = 0;

			
			map_status_report_request_t *p_v2_unstructured_ss_notify_req = NULL,*p_v2_unstructured_ss_notify_req_temp = NULL;
			p_v2_unstructured_ss_notify_req = (map_status_report_request_t *)\
			app_mem_get (sizeof (map_status_report_request_t));
			map_memzero(p_v2_unstructured_ss_notify_req, sizeof (map_status_report_request_t));

			fill_api_header(&(send_api->header), vlr_user_id);
			send_api->header.api_id = MAP_STATUS_REPORT_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 3;
			send_api->header.len = sizeof(map_status_report_request_t);

			fill_header(&p_v2_unstructured_ss_notify_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_STATUS_REPORT_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

                        token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
								p_v2_unstructured_ss_notify_req->arg.imsi.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.imsi.value[counter] = hstoi(token_generic);
							}
                            else if(index == 1)
                                p_v2_unstructured_ss_notify_req->arg.is_event_report_data= atoi(token_generic);
							else if(index == 2)
								p_v2_unstructured_ss_notify_req->arg.event_report_data.is_ccbs_subscriber_status = atoi(token_generic);
							else if(index == 3)
                            {
                                p_v2_unstructured_ss_notify_req->arg.event_report_data.ccbs_subscriber_status = atoi(token_generic);
                            }    
							else if(index == 4)
								p_v2_unstructured_ss_notify_req->arg.event_report_data.is_extension = atoi(token_generic);
							else if(index == 5)
							{
								p_v2_unstructured_ss_notify_req->arg.event_report_data.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter \
                                <p_v2_unstructured_ss_notify_req->arg.event_report_data.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_v2_unstructured_ss_notify_req->arg.event_report_data.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.event_report_data.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 6)
							{
								
							  for(ext_counter = 0; ext_counter <  \
                              p_v2_unstructured_ss_notify_req->arg.event_report_data.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_v2_unstructured_ss_notify_req->arg.event_report_data.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_v2_unstructured_ss_notify_req->arg.event_report_data.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_v2_unstructured_ss_notify_req->arg.event_report_data.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.event_report_data.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
							}
                            else if(index == 7)
								p_v2_unstructured_ss_notify_req->arg.is_call_reportdata = atoi(token_generic);
							else if(index == 8)
                            {
                                p_v2_unstructured_ss_notify_req->arg.call_reportdata.is_monitoring_mode = atoi(token_generic);
                            }
                            else if(index == 9)
								p_v2_unstructured_ss_notify_req->arg.call_reportdata.monitoring_mode = atoi(token_generic);
                            else if(index == 10)
								p_v2_unstructured_ss_notify_req->arg.call_reportdata.is_call_outcome = atoi(token_generic);
                            else if(index == 11)
								p_v2_unstructured_ss_notify_req->arg.call_reportdata.call_outcome = atoi(token_generic);

                            else if(index == 12)
								p_v2_unstructured_ss_notify_req->arg.call_reportdata.is_extension = atoi(token_generic);
							else if(index == 13)
							{
								p_v2_unstructured_ss_notify_req->arg.call_reportdata.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter \
                                <p_v2_unstructured_ss_notify_req->arg.call_reportdata.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_v2_unstructured_ss_notify_req->arg.call_reportdata.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.call_reportdata.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 14)
							{
								
							  for(ext_counter = 0; ext_counter <  \
                              p_v2_unstructured_ss_notify_req->arg.call_reportdata.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_v2_unstructured_ss_notify_req->arg.call_reportdata.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_v2_unstructured_ss_notify_req->arg.call_reportdata.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_v2_unstructured_ss_notify_req->arg.call_reportdata.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.call_reportdata.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
							}    

                            else if(index == 15)
								p_v2_unstructured_ss_notify_req->arg.is_extension = atoi(token_generic);
							else if(index == 16)
							{
								p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter \
                                <p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 17)
							{
								
							  for(ext_counter = 0; ext_counter <  \
                              p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp = (map_status_report_request_t *)\
					app_mem_get(sizeof (map_status_report_request_t));
					map_memzero(p_v2_unstructured_ss_notify_req_temp, sizeof (map_status_report_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v2_unstructured_ss_notify_req_temp,p_v2_unstructured_ss_notify_req,sizeof (map_status_report_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
                    {
						p_v2_unstructured_ss_notify_req_temp->header.last_component = 1;
					//	p_v2_unstructured_ss_notify_req_temp->header.last_component = 0;
                    }

                    //p_v2_unstructured_ss_notify_req_temp->header.corr_id =global_corr_id; 
					send_api_temp->p_data = p_v2_unstructured_ss_notify_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

                    //send_delimiter_request(p_v2_unstructured_ss_notify_req_temp->header.corr_id);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_v2_unstructured_ss_notify_req);


		}

        else
        if(!strncmp(api_name,"MAP_REMOTE_USER_FREE_REQUEST",strlen("MAP_REMOTE_USER_FREE_REQUEST"))) 
		{

			index = 0;

			
			map_remote_user_free_request_t *p_v2_unstructured_ss_notify_req = NULL,*p_v2_unstructured_ss_notify_req_temp = NULL;
			p_v2_unstructured_ss_notify_req = (map_remote_user_free_request_t *)\
			app_mem_get (sizeof (map_remote_user_free_request_t));
			map_memzero(p_v2_unstructured_ss_notify_req, sizeof (map_remote_user_free_request_t));

			fill_api_header(&(send_api->header), hlr_user_id);
			send_api->header.api_id = MAP_REMOTE_USER_FREE_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 3;
			send_api->header.len = sizeof(map_remote_user_free_request_t);

			fill_header(&p_v2_unstructured_ss_notify_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_REMOTE_USER_FREE_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

                        token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
								p_v2_unstructured_ss_notify_req->arg.imsi.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.imsi.value[counter] = hstoi(token_generic);
							}
                            else if(index == 1)
                                p_v2_unstructured_ss_notify_req->arg.call_info.protocol_id= atoi(token_generic);
							else if(index == 2)
							{

								p_v2_unstructured_ss_notify_req->arg.call_info.signal_info.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.call_info.signal_info.value[counter] = hstoi(token_generic);

							}	

                            else if(index == 3)
								p_v2_unstructured_ss_notify_req->arg.call_info.is_extension = atoi(token_generic);
							else if(index == 4)
							{
								p_v2_unstructured_ss_notify_req->arg.call_info.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter \
                                <p_v2_unstructured_ss_notify_req->arg.call_info.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_v2_unstructured_ss_notify_req->arg.call_info.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.call_info.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 5)
							{
								
							  for(ext_counter = 0; ext_counter <  \
                              p_v2_unstructured_ss_notify_req->arg.call_info.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_v2_unstructured_ss_notify_req->arg.call_info.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_v2_unstructured_ss_notify_req->arg.call_info.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_v2_unstructured_ss_notify_req->arg.call_info.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.call_info.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
							}
							else if(index == 6)
								p_v2_unstructured_ss_notify_req->arg.ccbs_feature.is_ccbs_index = atoi(token_generic);
							else if(index == 7)
							{
								p_v2_unstructured_ss_notify_req->arg.ccbs_feature.ccbs_index=hstoi(token_generic); 
							}

                            else if(index == 8)
								p_v2_unstructured_ss_notify_req->arg.ccbs_feature.is_b_subscriber_number = atoi(token_generic);
                            else if(index == 9)
                            {
								p_v2_unstructured_ss_notify_req->arg.ccbs_feature.b_subscriber_number.length= atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.ccbs_feature.b_subscriber_number.value[counter]= hstoi(token_generic);

                            }

                            else if(index == 10)
								p_v2_unstructured_ss_notify_req->arg.ccbs_feature.is_b_subscriber_subaddress = atoi(token_generic);
                            else if(index == 11)
                            {
								p_v2_unstructured_ss_notify_req->arg.ccbs_feature.b_subscriber_subaddress.length= atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.ccbs_feature.b_subscriber_subaddress.value[counter]= hstoi(token_generic);

                            }

                            else if(index == 12)
								p_v2_unstructured_ss_notify_req->arg.ccbs_feature.is_basic_service_group = atoi(token_generic);
                            else if(index == 13)
								p_v2_unstructured_ss_notify_req->arg.ccbs_feature.basic_service_group.choice = atoi(token_generic);


                            else if(index == 14)
                            {

                                if(p_v2_unstructured_ss_notify_req->arg.ccbs_feature.basic_service_group.choice == 1)
                                {
								p_v2_unstructured_ss_notify_req->arg.ccbs_feature.basic_service_group.u.bearer_service.length= atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.ccbs_feature.basic_service_group.u.bearer_service.value[counter]= hstoi(token_generic);
                                }
                            }
                            else if(index == 15)
                            {
								p_v2_unstructured_ss_notify_req->arg.translated_b_number.length= atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.translated_b_number.value[counter]= hstoi(token_generic);
                            }
                            else if(index == 16)
								p_v2_unstructured_ss_notify_req->arg.is_replace_b_number = atoi(token_generic);
                            else if(index == 17)
								p_v2_unstructured_ss_notify_req->arg.replace_b_number= hstoi(token_generic);
                            else if(index == 18)
								p_v2_unstructured_ss_notify_req->arg.is_alerting_pattern = atoi(token_generic);
                            else if(index == 19)
                            {
								p_v2_unstructured_ss_notify_req->arg.alerting_pattern.length= atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.alerting_pattern.value[counter]= hstoi(token_generic);

                            }
                            else if(index == 20)
								p_v2_unstructured_ss_notify_req->arg.is_extension = atoi(token_generic);
							else if(index == 21)
							{
								p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter \
                                <p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 22)
							{
								
							  for(ext_counter = 0; ext_counter <  \
                              p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp = (map_remote_user_free_request_t *)\
					app_mem_get(sizeof (map_remote_user_free_request_t));
					map_memzero(p_v2_unstructured_ss_notify_req_temp, sizeof (map_remote_user_free_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v2_unstructured_ss_notify_req_temp,p_v2_unstructured_ss_notify_req,sizeof (map_remote_user_free_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
                    {
						p_v2_unstructured_ss_notify_req_temp->header.last_component = 1;
					//	p_v2_unstructured_ss_notify_req_temp->header.last_component = 0;
                    }

                    //p_v2_unstructured_ss_notify_req_temp->header.corr_id =global_corr_id; 
					send_api_temp->p_data = p_v2_unstructured_ss_notify_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

                    //send_delimiter_request(p_v2_unstructured_ss_notify_req_temp->header.corr_id);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_v2_unstructured_ss_notify_req);


		}

        else
        if(!strncmp(api_name,"MAP_IST_ALERT_REQUEST",strlen("MAP_IST_ALERT_REQUEST"))) 
		{

			index = 0;

			
			map_ist_alert_request_t *p_v2_unstructured_ss_notify_req = NULL,*p_v2_unstructured_ss_notify_req_temp = NULL;
			p_v2_unstructured_ss_notify_req = (map_ist_alert_request_t *)\
			app_mem_get (sizeof (map_ist_alert_request_t));
			map_memzero(p_v2_unstructured_ss_notify_req, sizeof (map_ist_alert_request_t));

			fill_api_header(&(send_api->header), msc_user_id);
			send_api->header.api_id = MAP_IST_ALERT_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 3;
			send_api->header.len = sizeof(map_ist_alert_request_t);

			fill_header(&p_v2_unstructured_ss_notify_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_IST_ALERT_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

                        token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
								p_v2_unstructured_ss_notify_req->arg.imsi.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.imsi.value[counter] = hstoi(token_generic);
							}
                            else if(index == 1)
								p_v2_unstructured_ss_notify_req->arg.is_extension = atoi(token_generic);
							else if(index == 2)
							{
								p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter \
                                <p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 3)
							{
								
							  for(ext_counter = 0; ext_counter <  \
                              p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp = (map_ist_alert_request_t *)\
					app_mem_get(sizeof (map_ist_alert_request_t));
					map_memzero(p_v2_unstructured_ss_notify_req_temp, sizeof (map_ist_alert_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v2_unstructured_ss_notify_req_temp,p_v2_unstructured_ss_notify_req,sizeof (map_ist_alert_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
                    {
						p_v2_unstructured_ss_notify_req_temp->header.last_component = 1;
					//	p_v2_unstructured_ss_notify_req_temp->header.last_component = 0;
                    }

                    //p_v2_unstructured_ss_notify_req_temp->header.corr_id =global_corr_id; 
					send_api_temp->p_data = p_v2_unstructured_ss_notify_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

                    //send_delimiter_request(p_v2_unstructured_ss_notify_req_temp->header.corr_id);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_v2_unstructured_ss_notify_req);


		}

        else
        if(!strncmp(api_name,"MAP_IST_COMMAND_REQUEST",strlen("MAP_IST_COMMAND_REQUEST"))) 
		{

			index = 0;

			
			map_ist_command_request_t *p_v2_unstructured_ss_notify_req = NULL,*p_v2_unstructured_ss_notify_req_temp = NULL;
			p_v2_unstructured_ss_notify_req = (map_ist_command_request_t *)\
			app_mem_get (sizeof (map_ist_command_request_t));
			map_memzero(p_v2_unstructured_ss_notify_req, sizeof (map_ist_command_request_t));

			fill_api_header(&(send_api->header), hlr_user_id);
			send_api->header.api_id = MAP_IST_COMMAND_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 3;
			send_api->header.len = sizeof(map_ist_command_request_t);

			fill_header(&p_v2_unstructured_ss_notify_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_IST_COMMAND_REQUEST is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

                        token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
								p_v2_unstructured_ss_notify_req->arg.imsi.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.imsi.value[counter] = hstoi(token_generic);
							}
                            else if(index == 1)
								p_v2_unstructured_ss_notify_req->arg.is_extension = atoi(token_generic);
							else if(index == 2)
							{
								p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter \
                                <p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 3)
							{
								
							  for(ext_counter = 0; ext_counter <  \
                              p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

                    

					printf("Sending The message \n");

                    if(seq_control == 15)
                         seq_control = 1;
                    else
                         seq_control++;
                        


					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp = (map_ist_command_request_t *)\
					app_mem_get(sizeof (map_ist_command_request_t));
					map_memzero(p_v2_unstructured_ss_notify_req_temp, sizeof (map_ist_command_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v2_unstructured_ss_notify_req_temp,p_v2_unstructured_ss_notify_req,sizeof (map_ist_command_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp->header.invoke_id = message_counter + 1;

					//if(message_counter == (num_of_messages -1))
                    {
						p_v2_unstructured_ss_notify_req_temp->header.last_component = 1;
					//	p_v2_unstructured_ss_notify_req_temp->header.last_component = 0;
                    }

                    //p_v2_unstructured_ss_notify_req_temp->header.corr_id =global_corr_id; 
					send_api_temp->p_data = p_v2_unstructured_ss_notify_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

                    sleep(1);

                    //send_delimiter_request(p_v2_unstructured_ss_notify_req_temp->header.corr_id);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_v2_unstructured_ss_notify_req);


		}
        else if(!strncmp(api_name,"MAP_RESTORE_DATA_REQUEST",strlen("MAP_RESTORE_DATA_REQUEST"))) 
		{

			index = 0;

			
			map_restore_data_request_t *p_v2_unstructured_ss_notify_req = NULL,*p_v2_unstructured_ss_notify_req_temp = NULL;
			p_v2_unstructured_ss_notify_req = (map_restore_data_request_t *)\
			app_mem_get (sizeof (map_restore_data_request_t));
			map_memzero(p_v2_unstructured_ss_notify_req, sizeof (map_restore_data_request_t));

			fill_api_header(&(send_api->header), vlr_user_id);
			send_api->header.api_id = MAP_RESTORE_DATA_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 3;
			send_api->header.len = sizeof(map_restore_data_request_t);

			fill_header(&p_v2_unstructured_ss_notify_req->header,corr_id);


			fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
			memset(bitmap_array,'\0',20);
			strncpy(bitmap_array,line_val,20);

			while(1)
			{
				if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

					printf("MAP_RESTORE_DATA_REQUEST  is successfully Parsed\n\n");
					break;
				}

				if ((*line_val == ' ') ||
						(*line_val == '#') ||
						(*line_val == '\n')) {
					continue;
				}
				else{
					//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
					//if(bitmap_array[index] == '1')
                    if(1)
					{
						token_length = strtok(line_val," ");
						token_val = strtok(NULL," ");

                        token_val_temp = (char *)malloc(strlen(token_val));
						strcpy(token_val_temp,token_val);

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
								p_v2_unstructured_ss_notify_req->arg.imsi.length = atoi(token_length);
								p_v2_unstructured_ss_notify_req->arg.imsi.value[counter] = hstoi(token_generic);
							}
							else if(index == 1)
							{
								p_v2_unstructured_ss_notify_req->arg.is_lmsi = atoi(token_generic);
							}	
							else if(index == 2)
							{
								//p_v2_unstructured_ss_notify_req->arg.lmsi.length  =atoi(token_length) *8;
								p_v2_unstructured_ss_notify_req->arg.lmsi.length  =atoi(token_length) ;
								p_v2_unstructured_ss_notify_req->arg.lmsi.value[counter] =hstoi(token_generic);
							}
                            else if(index == 3)
								p_v2_unstructured_ss_notify_req->arg.is_extension = atoi(token_generic);
							else if(index == 4)
							{
								p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter \
                                <p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 5)
							{
								
							  for(ext_counter = 0; ext_counter <  \
                              p_v2_unstructured_ss_notify_req->arg.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
							}
							else if(index == 6)
							p_v2_unstructured_ss_notify_req->arg.is_vlr_capability = atoi(token_generic);

							else if(index == 7)
							{
								p_v2_unstructured_ss_notify_req->arg.vlr_capability.is_supported_camel_phases =atoi(token_generic); 
							}
                            else if(index == 8)
                            {

								p_v2_unstructured_ss_notify_req->arg.vlr_capability.supported_camel_phases.length=atoi(token_length)*8;

								p_v2_unstructured_ss_notify_req->arg.vlr_capability.supported_camel_phases.value[counter]=hstoi(token_generic);
                            }

                            else if(index == 9)
								p_v2_unstructured_ss_notify_req->arg.vlr_capability.is_extension = atoi(token_generic);
							else if(index == 10)
							{
								p_v2_unstructured_ss_notify_req->arg.vlr_capability.extension_container.map_pvt_ext_count = atoi(token_length);

								for(ext_counter = 0; ext_counter \
                                <p_v2_unstructured_ss_notify_req->arg.vlr_capability.extension_container.map_pvt_ext_count;ext_counter++ )
								{
									int finished_flag = 0;
									if(ext_counter ==0)	
									{
										token_ext = strtok(token_val_temp,":");
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
												p_v2_unstructured_ss_notify_req->arg.vlr_capability.extension_container.private_ext_list[ext_counter].ext_obj_id.length = atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.vlr_capability.extension_container.private_ext_list[ext_counter].ext_obj_id.value[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
									
					
								}

							}
							else if(index == 11)
							{
								
							  for(ext_counter = 0; ext_counter <  \
                              p_v2_unstructured_ss_notify_req->arg.vlr_capability.extension_container.map_pvt_ext_count;ext_counter++ )
							  {
								int finished_flag = 0;
								if(ext_counter ==0)	
								{
									token_ext = strtok(token_val_temp,":");
								}		
								else{
									token_ext = strtok(NULL,":");
								}


								if(!strncmp(token_ext,"NULL",strlen("NULL")))
									p_v2_unstructured_ss_notify_req->arg.vlr_capability.extension_container.private_ext_list[ext_counter].is_ext_type = 0;
								else
								{	
									p_v2_unstructured_ss_notify_req->arg.vlr_capability.extension_container.private_ext_list[ext_counter].is_ext_type = 1;
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
												p_v2_unstructured_ss_notify_req->arg.vlr_capability.extension_container.private_ext_list[ext_counter].enc_len= atoi(token_generic);


											}
											else
											{
												p_v2_unstructured_ss_notify_req->arg.vlr_capability.extension_container.private_ext_list[ext_counter].encoded[finished_flag - 1] = hstoi(token_generic);
											}
										}

										finished_flag++;

									}
								}

							   }
							}
                            else if(index == 12)
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.is_solsa_support_indicator = atoi(token_generic);
                            else if(index == 13)
                            {
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.solsa_support_indicator  = hstoi(token_generic);

                            }   
                            else if(index == 14)
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.is_ist_support_indicator = atoi(token_generic);
                            else if(index == 15)
                            {
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.ist_support_indicator = atoi(token_generic);
                            }   
                            else if(index == 16)
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.is_super_charger_supported_in_serving_network_entity = atoi(token_generic);
                            else if(index == 17)
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.super_charger_supported_in_serving_network_entity.choice = atoi(token_generic);
                                
                            else if(index == 18)
                            {
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.super_charger_supported_in_serving_network_entity.u.send_subscriber_data = hstoi(token_generic);
                            }   
                            else if(index == 19)
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.is_long_ftn_supported = atoi(token_generic);
                            else if(index == 20)
                            {
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.long_ftn_supported = hstoi(token_generic);
                            }
                            else if(index == 21)
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.is_supported_lcs_capability_sets = atoi(token_generic);
                            else if(index == 22)
                            {
                             //p_v2_unstructured_ss_notify_req->arg.vlr_capability.supported_lcs_capability_sets.length = atoi(token_length);
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.supported_lcs_capability_sets.length  = atoi(token_length)*8;
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.supported_lcs_capability_sets.value[counter]= hstoi(token_generic); 
                            }
                            else if(index == 23)
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.is_offered_camel4_csis = atoi(token_generic);
                            else if(index == 24)
                            {
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.offered_camel4_csis.length= atoi(token_length) *8;
                             //p_v2_unstructured_ss_notify_req->arg.vlr_capability.offered_camel4_csis.length= atoi(token_length) ;
                             p_v2_unstructured_ss_notify_req->arg.vlr_capability.offered_camel4_csis.value[counter]= hstoi(token_generic); 
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


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp = (map_restore_data_request_t *)\
					app_mem_get(sizeof (map_restore_data_request_t));
					map_memzero(p_v2_unstructured_ss_notify_req_temp, sizeof (map_restore_data_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v2_unstructured_ss_notify_req_temp,p_v2_unstructured_ss_notify_req,sizeof (map_restore_data_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
						p_v2_unstructured_ss_notify_req_temp->header.last_component = 1;

					send_api_temp->p_data = p_v2_unstructured_ss_notify_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_v2_unstructured_ss_notify_req);


		}

        else
        if(!strncmp(api_name,"MAP_INSERT_SUBSCRIBER_DATA_REQUEST",strlen("MAP_INSERT_SUBSCRIBER_DATA_REQUEST"))) 
		{

			index = 0;

			
			map_insert_subscriber_data_request_t *p_v2_unstructured_ss_notify_req = NULL,*p_v2_unstructured_ss_notify_req_temp = NULL;
			p_v2_unstructured_ss_notify_req = (map_insert_subscriber_data_request_t *)\
			app_mem_get (sizeof (map_insert_subscriber_data_request_t));
			map_memzero(p_v2_unstructured_ss_notify_req, sizeof (map_insert_subscriber_data_request_t));

			fill_api_header(&(send_api->header), hlr_user_id);
			send_api->header.api_id = MAP_INSERT_SUBSCRIBER_DATA_REQUEST;
			send_api->header.spare1 = g_sap;
			send_api->header.ver = 3;
			send_api->header.len = sizeof(map_insert_subscriber_data_request_t);

			fill_header(&p_v2_unstructured_ss_notify_req->header,corr_id);


            //Filling the Buffer value

            fill_isd_req_argument(p_v2_unstructured_ss_notify_req);

			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < num_of_messages;message_counter++ )
				{

					printf("Sending The message \n");
					send_api_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp = (map_insert_subscriber_data_request_t *)\
					app_mem_get(sizeof (map_insert_subscriber_data_request_t));
					map_memzero(p_v2_unstructured_ss_notify_req_temp, sizeof (map_insert_subscriber_data_request_t));



					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{
						corr_id = app_map_get_new_correlation_id();
						p_v2_unstructured_ss_notify_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
					}

					memcpy(p_v2_unstructured_ss_notify_req_temp,p_v2_unstructured_ss_notify_req,sizeof (map_insert_subscriber_data_request_t));

					memcpy(send_api_temp,send_api,sizeof(map_api_struct_t));

					p_v2_unstructured_ss_notify_req_temp->header.invoke_id = message_counter + 1;

					if(message_counter == (num_of_messages -1))
                    {
						p_v2_unstructured_ss_notify_req_temp->header.last_component = 1;
					//	p_v2_unstructured_ss_notify_req_temp->header.last_component = 0;
                    }

                    //p_v2_unstructured_ss_notify_req_temp->header.corr_id =global_corr_id; 
					send_api_temp->p_data = p_v2_unstructured_ss_notify_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_temp, &error);

                    //send_delimiter_request(p_v2_unstructured_ss_notify_req_temp->header.corr_id);

				}

			}

			app_mem_free(send_api);
			app_mem_free(p_v2_unstructured_ss_notify_req);


		}

	else
        if(!strncmp(api_name,"MAP_LOCATION_UPDATE_REQUEST",strlen("MAP_LOCATION_UPDATE_REQUEST"))) 
		{

			index = 0;

			if(send_api_lu == NULL)
			{
				send_api_lu = app_mem_get(sizeof(map_api_struct_t));
				map_memzero(send_api_lu,sizeof(map_api_struct_t));

				sprintf(filename,"../buffers/%s",api_name);
				fp_apis = fopen(filename,"r");


			}
					
			if(p_update_loc_req == NULL)
			{
				p_update_loc_req = (map_update_location_request_t *)\
						   app_mem_get (sizeof (map_update_location_request_t));
				map_memzero(p_update_loc_req, sizeof (map_update_location_request_t));

				fill_api_header(&(send_api_lu->header), vlr_user_id);
				send_api_lu->header.api_id = MAP_UPDATE_LOCATION_REQUEST;
				send_api_lu->header.spare1 = g_sap;
				send_api_lu->header.ver = 3;
				send_api_lu->header.len = sizeof(map_update_location_request_t);

				fill_header(&p_update_loc_req->header,corr_id);


				fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
				memset(bitmap_array,'\0',20);
				strncpy(bitmap_array,line_val,20);

				while(1)
				{
					if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

						printf("MAP_LOCATION_UPDATE_REQUEST is successfully Parsed\n\n");
						break;
					}

					if ((*line_val == ' ') ||
							(*line_val == '#') ||
							(*line_val == '\n')) {
						continue;
					}
					else{
						//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
						//if(bitmap_array[index] == '1')
						if(1)
						{
							token_length = strtok(line_val," ");
							token_val = strtok(NULL," ");

							token_val_temp = (char *)malloc(strlen(token_val));
							strncpy(token_val_temp,token_val,strlen(token_val));

							//printf("Token Length [%s] \n",token_length);
							//printf("Token Value [%s] \n",token_val);
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
									p_update_loc_req->arg.imsi.length = atoi(token_length);
									p_update_loc_req->arg.imsi.value[counter] = hstoi(token_generic);
								}
								else if(index == 1)
								{
									p_update_loc_req->arg.msc_number.length = atoi(token_length);
									p_update_loc_req->arg.msc_number.value[counter] = hstoi(token_generic);
								}
								else if(index == 2)
								{
									p_update_loc_req->arg.vlr_number.length = atoi(token_length);
									p_update_loc_req->arg.vlr_number.value[counter] = hstoi(token_generic);
								}
								else if(index == 3)
								{
									p_update_loc_req->arg.is_lmsi = atoi(token_generic);
								}	
								else if(index == 4)
								{
									//p_v2_unstructured_ss_notify_req->arg.lmsi.length  =atoi(token_length) *8;
									p_update_loc_req->arg.lmsi.length  =atoi(token_length) ;
									p_update_loc_req->arg.lmsi.value[counter] =hstoi(token_generic);
								}
								else if(index == 5)
									p_update_loc_req->arg.is_extension = atoi(token_generic);
								else if(index == 6)
									p_update_loc_req->arg.is_vlr_capability = atoi(token_generic);
								else if(index == 7)
								{
									p_update_loc_req->arg.vlr_capability.is_supported_camel_phases =atoi(token_generic); 
								}
								else if(index == 8)
								{

									p_update_loc_req->arg.vlr_capability.supported_camel_phases.length=atoi(token_length)*8;

									p_update_loc_req->arg.vlr_capability.supported_camel_phases.value[counter]=hstoi(token_generic);
								}
								else if(index == 9)
									p_update_loc_req->arg.vlr_capability.is_ist_support_indicator = atoi(token_generic);
								else if(index == 10)
								{
									p_update_loc_req->arg.vlr_capability.ist_support_indicator = atoi(token_generic);
								}   
								else if(index == 11)
									p_update_loc_req->arg.vlr_capability.is_long_ftn_supported = atoi(token_generic);
								else if(index == 12)
								{
									p_update_loc_req->arg.vlr_capability.long_ftn_supported = hstoi(token_generic);
								}



							}

							index++;
							free(token_val_temp);

						}
						else{

							printf("No Optional Parameter is there so Please check\n");
							break;
						}

					}

				}

				fclose(fp_apis);

			}



			p_update_loc_req->arg.vlr_capability.is_extension = 0;
			p_update_loc_req->arg.vlr_capability.is_solsa_support_indicator = 0;
			p_update_loc_req->arg.vlr_capability.is_super_charger_supported_in_serving_network_entity=0;
			p_update_loc_req->arg.vlr_capability.is_supported_lcs_capability_sets = 0;
			p_update_loc_req->arg.vlr_capability.is_offered_camel4_csis = 0;
			p_update_loc_req->arg.is_inform_previous_network_entity = 0;
			p_update_loc_req->arg.is_cs_lcs_not_supported_by_ue = 0;


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < no_mess_in_burst;message_counter++ )
				{

					//printf("Sending The message \n");
					send_api_lu_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					//map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_update_loc_req_temp = (map_update_location_request_t *)\
					app_mem_get(sizeof (map_update_location_request_t));
				//	map_memzero(p_update_loc_req_temp, sizeof (map_update_location_request_t));



					#if 0

					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_update_loc_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{

					#endif
						corr_id = app_map_get_new_correlation_id();
						p_update_loc_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
				//	}

					memcpy(p_update_loc_req_temp,p_update_loc_req,sizeof (map_update_location_request_t));

					memcpy(send_api_lu_temp,send_api_lu,sizeof(map_api_struct_t));

					p_update_loc_req_temp->header.invoke_id = message_counter + 1;

					//if(message_counter == (num_of_messages -1))
                    {
						p_update_loc_req_temp->header.last_component = 1;
					//	p_v2_unstructured_ss_notify_req_temp->header.last_component = 0;
                    }

                    //p_v2_unstructured_ss_notify_req_temp->header.corr_id =global_corr_id; 
					send_api_lu_temp->p_data = p_update_loc_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_lu_temp, &error);

					lu_request_counters++;

                    //send_delimiter_request(p_v2_unstructured_ss_notify_req_temp->header.corr_id);

				}

				usleep(delay);

			}

			#if 0
			app_mem_free(send_api_lu);
			app_mem_free(p_update_loc_req);

			#endif


	}



        else
        if(!strncmp(api_name,"MAP_LOCATION_UPDATE_REQUEST",strlen("MAP_LOCATION_UPDATE_REQUEST"))) 
		{

			index = 0;

			if(send_api_lu == NULL)
			{
				send_api_lu = app_mem_get(sizeof(map_api_struct_t));
				map_memzero(send_api_lu,sizeof(map_api_struct_t));

				sprintf(filename,"../buffers/%s",api_name);
				fp_apis = fopen(filename,"r");


			}
					
			if(p_update_loc_req == NULL)
			{
				p_update_loc_req = (map_update_location_request_t *)\
						   app_mem_get (sizeof (map_update_location_request_t));
				map_memzero(p_update_loc_req, sizeof (map_update_location_request_t));

				fill_api_header(&(send_api_lu->header), vlr_user_id);
				send_api_lu->header.api_id = MAP_UPDATE_LOCATION_REQUEST;
				send_api_lu->header.spare1 = g_sap;
				send_api_lu->header.ver = 3;
				send_api_lu->header.len = sizeof(map_update_location_request_t);

				fill_header(&p_update_loc_req->header,corr_id);


				fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
				memset(bitmap_array,'\0',20);
				strncpy(bitmap_array,line_val,20);

				while(1)
				{
					if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

						printf("MAP_LOCATION_UPDATE_REQUEST is successfully Parsed\n\n");
						break;
					}

					if ((*line_val == ' ') ||
							(*line_val == '#') ||
							(*line_val == '\n')) {
						continue;
					}
					else{
						//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
						//if(bitmap_array[index] == '1')
						if(1)
						{
							token_length = strtok(line_val," ");
							token_val = strtok(NULL," ");

							token_val_temp = (char *)malloc(strlen(token_val));
							strncpy(token_val_temp,token_val,strlen(token_val));

							//printf("Token Length [%s] \n",token_length);
							//printf("Token Value [%s] \n",token_val);
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
									p_update_loc_req->arg.imsi.length = atoi(token_length);
									p_update_loc_req->arg.imsi.value[counter] = hstoi(token_generic);
								}
								else if(index == 1)
								{
									p_update_loc_req->arg.msc_number.length = atoi(token_length);
									p_update_loc_req->arg.msc_number.value[counter] = hstoi(token_generic);
								}
								else if(index == 2)
								{
									p_update_loc_req->arg.vlr_number.length = atoi(token_length);
									p_update_loc_req->arg.vlr_number.value[counter] = hstoi(token_generic);
								}
								else if(index == 3)
								{
									p_update_loc_req->arg.is_lmsi = atoi(token_generic);
								}	
								else if(index == 4)
								{
									//p_v2_unstructured_ss_notify_req->arg.lmsi.length  =atoi(token_length) *8;
									p_update_loc_req->arg.lmsi.length  =atoi(token_length) ;
									p_update_loc_req->arg.lmsi.value[counter] =hstoi(token_generic);
								}
								else if(index == 5)
									p_update_loc_req->arg.is_extension = atoi(token_generic);
								else if(index == 6)
									p_update_loc_req->arg.is_vlr_capability = atoi(token_generic);
								else if(index == 7)
								{
									p_update_loc_req->arg.vlr_capability.is_supported_camel_phases =atoi(token_generic); 
								}
								else if(index == 8)
								{

									p_update_loc_req->arg.vlr_capability.supported_camel_phases.length=atoi(token_length)*8;

									p_update_loc_req->arg.vlr_capability.supported_camel_phases.value[counter]=hstoi(token_generic);
								}
								else if(index == 9)
									p_update_loc_req->arg.vlr_capability.is_ist_support_indicator = atoi(token_generic);
								else if(index == 10)
								{
									p_update_loc_req->arg.vlr_capability.ist_support_indicator = atoi(token_generic);
								}   
								else if(index == 11)
									p_update_loc_req->arg.vlr_capability.is_long_ftn_supported = atoi(token_generic);
								else if(index == 12)
								{
									p_update_loc_req->arg.vlr_capability.long_ftn_supported = hstoi(token_generic);
								}



							}

							index++;
							free(token_val_temp);

						}
						else{

							printf("No Optional Parameter is there so Please check\n");
							break;
						}

					}

				}

				fclose(fp_apis);

			}



			p_update_loc_req->arg.vlr_capability.is_extension = 0;
			p_update_loc_req->arg.vlr_capability.is_solsa_support_indicator = 0;
			p_update_loc_req->arg.vlr_capability.is_super_charger_supported_in_serving_network_entity=0;
			p_update_loc_req->arg.vlr_capability.is_supported_lcs_capability_sets = 0;
			p_update_loc_req->arg.vlr_capability.is_offered_camel4_csis = 0;
			p_update_loc_req->arg.is_inform_previous_network_entity = 0;
			p_update_loc_req->arg.is_cs_lcs_not_supported_by_ue = 0;


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < no_mess_in_burst;message_counter++ )
				{

					//printf("Sending The message \n");
					send_api_lu_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					//map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_update_loc_req_temp = (map_update_location_request_t *)\
					app_mem_get(sizeof (map_update_location_request_t));
				//	map_memzero(p_update_loc_req_temp, sizeof (map_update_location_request_t));



					#if 0

					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_update_loc_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{

					#endif
						corr_id = app_map_get_new_correlation_id();
						p_update_loc_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
				//	}

					memcpy(p_update_loc_req_temp,p_update_loc_req,sizeof (map_update_location_request_t));

					memcpy(send_api_lu_temp,send_api_lu,sizeof(map_api_struct_t));

					p_update_loc_req_temp->header.invoke_id = message_counter + 1;

					//if(message_counter == (num_of_messages -1))
                    {
						p_update_loc_req_temp->header.last_component = 1;
					//	p_v2_unstructured_ss_notify_req_temp->header.last_component = 0;
                    }

                    //p_v2_unstructured_ss_notify_req_temp->header.corr_id =global_corr_id; 
					send_api_lu_temp->p_data = p_update_loc_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_lu_temp, &error);

					lu_request_counters++;

                    //send_delimiter_request(p_v2_unstructured_ss_notify_req_temp->header.corr_id);

				}

				usleep(delay);

			}

			#if 0
			app_mem_free(send_api_lu);
			app_mem_free(p_update_loc_req);

			#endif


	}
else
        if(!strncmp(api_name,"MAP_LOCATION_UPDATE_REQUEST",strlen("MAP_LOCATION_UPDATE_REQUEST"))) 
		{

			index = 0;

			if(send_api_lu == NULL)
			{
				send_api_lu = app_mem_get(sizeof(map_api_struct_t));
				map_memzero(send_api_lu,sizeof(map_api_struct_t));

				sprintf(filename,"../buffers/%s",api_name);
				fp_apis = fopen(filename,"r");


			}
					
			if(p_update_loc_req == NULL)
			{
				p_update_loc_req = (map_update_location_request_t *)\
						   app_mem_get (sizeof (map_update_location_request_t));
				map_memzero(p_update_loc_req, sizeof (map_update_location_request_t));

				fill_api_header(&(send_api_lu->header), vlr_user_id);
				send_api_lu->header.api_id = MAP_UPDATE_LOCATION_REQUEST;
				send_api_lu->header.spare1 = g_sap;
				send_api_lu->header.ver = 3;
				send_api_lu->header.len = sizeof(map_update_location_request_t);

				fill_header(&p_update_loc_req->header,corr_id);


				fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
				memset(bitmap_array,'\0',20);
				strncpy(bitmap_array,line_val,20);

				while(1)
				{
					if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

						printf("MAP_LOCATION_UPDATE_REQUEST is successfully Parsed\n\n");
						break;
					}

					if ((*line_val == ' ') ||
							(*line_val == '#') ||
							(*line_val == '\n')) {
						continue;
					}
					else{
						//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
						//if(bitmap_array[index] == '1')
						if(1)
						{
							token_length = strtok(line_val," ");
							token_val = strtok(NULL," ");

							token_val_temp = (char *)malloc(strlen(token_val));
							strncpy(token_val_temp,token_val,strlen(token_val));

							//printf("Token Length [%s] \n",token_length);
							//printf("Token Value [%s] \n",token_val);
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
									p_update_loc_req->arg.imsi.length = atoi(token_length);
									p_update_loc_req->arg.imsi.value[counter] = hstoi(token_generic);
								}
								else if(index == 1)
								{
									p_update_loc_req->arg.msc_number.length = atoi(token_length);
									p_update_loc_req->arg.msc_number.value[counter] = hstoi(token_generic);
								}
								else if(index == 2)
								{
									p_update_loc_req->arg.vlr_number.length = atoi(token_length);
									p_update_loc_req->arg.vlr_number.value[counter] = hstoi(token_generic);
								}
								else if(index == 3)
								{
									p_update_loc_req->arg.is_lmsi = atoi(token_generic);
								}	
								else if(index == 4)
								{
									//p_v2_unstructured_ss_notify_req->arg.lmsi.length  =atoi(token_length) *8;
									p_update_loc_req->arg.lmsi.length  =atoi(token_length) ;
									p_update_loc_req->arg.lmsi.value[counter] =hstoi(token_generic);
								}
								else if(index == 5)
									p_update_loc_req->arg.is_extension = atoi(token_generic);
								else if(index == 6)
									p_update_loc_req->arg.is_vlr_capability = atoi(token_generic);
								else if(index == 7)
								{
									p_update_loc_req->arg.vlr_capability.is_supported_camel_phases =atoi(token_generic); 
								}
								else if(index == 8)
								{

									p_update_loc_req->arg.vlr_capability.supported_camel_phases.length=atoi(token_length)*8;

									p_update_loc_req->arg.vlr_capability.supported_camel_phases.value[counter]=hstoi(token_generic);
								}
								else if(index == 9)
									p_update_loc_req->arg.vlr_capability.is_ist_support_indicator = atoi(token_generic);
								else if(index == 10)
								{
									p_update_loc_req->arg.vlr_capability.ist_support_indicator = atoi(token_generic);
								}   
								else if(index == 11)
									p_update_loc_req->arg.vlr_capability.is_long_ftn_supported = atoi(token_generic);
								else if(index == 12)
								{
									p_update_loc_req->arg.vlr_capability.long_ftn_supported = hstoi(token_generic);
								}



							}

							index++;
							free(token_val_temp);

						}
						else{

							printf("No Optional Parameter is there so Please check\n");
							break;
						}

					}

				}

				fclose(fp_apis);

			}



			p_update_loc_req->arg.vlr_capability.is_extension = 0;
			p_update_loc_req->arg.vlr_capability.is_solsa_support_indicator = 0;
			p_update_loc_req->arg.vlr_capability.is_super_charger_supported_in_serving_network_entity=0;
			p_update_loc_req->arg.vlr_capability.is_supported_lcs_capability_sets = 0;
			p_update_loc_req->arg.vlr_capability.is_offered_camel4_csis = 0;
			p_update_loc_req->arg.is_inform_previous_network_entity = 0;
			p_update_loc_req->arg.is_cs_lcs_not_supported_by_ue = 0;


			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < no_mess_in_burst;message_counter++ )
				{

					//printf("Sending The message \n");
					send_api_lu_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					//map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_update_loc_req_temp = (map_update_location_request_t *)\
					app_mem_get(sizeof (map_update_location_request_t));
				//	map_memzero(p_update_loc_req_temp, sizeof (map_update_location_request_t));



					#if 0

					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_update_loc_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{

					#endif
						corr_id = app_map_get_new_correlation_id();
						p_update_loc_req->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
				//	}

					memcpy(p_update_loc_req_temp,p_update_loc_req,sizeof (map_update_location_request_t));

					memcpy(send_api_lu_temp,send_api_lu,sizeof(map_api_struct_t));

					p_update_loc_req_temp->header.invoke_id = message_counter + 1;

					//if(message_counter == (num_of_messages -1))
                    {
						p_update_loc_req_temp->header.last_component = 1;
					//	p_v2_unstructured_ss_notify_req_temp->header.last_component = 0;
                    }

                    //p_v2_unstructured_ss_notify_req_temp->header.corr_id =global_corr_id; 
					send_api_lu_temp->p_data = p_update_loc_req_temp;
					app_map_send_to_app_map((unsigned char *)send_api_lu_temp, &error);

					lu_request_counters++;

                    //send_delimiter_request(p_v2_unstructured_ss_notify_req_temp->header.corr_id);

				}

				usleep(delay);

			}

			#if 0
			app_mem_free(send_api_lu);
			app_mem_free(p_update_loc_req);

			#endif


	}
else
        if(!strncmp(api_name,"MAP_V1_LOCATION_UPDATE_REQUEST",strlen("MAP_V1_LOCATION_UPDATE_REQUEST"))) 
		{

			index = 0;

			if(send_api_lu == NULL)
			{
				send_api_lu = app_mem_get(sizeof(map_api_struct_t));
				map_memzero(send_api_lu,sizeof(map_api_struct_t));

				sprintf(filename,"../buffers/%s",api_name);
				fp_apis = fopen(filename,"r");


			}
					
			if(p_update_loc_req_1 == NULL)
			{
				p_update_loc_req_1 = (map_v1_update_location_request_t *)\
						   app_mem_get (sizeof (map_v1_update_location_request_t));
				map_memzero(p_update_loc_req_1, sizeof (map_v1_update_location_request_t));

				fill_api_header(&(send_api_lu->header), vlr_user_id);
				send_api_lu->header.api_id = MAP_UPDATE_LOCATION_REQUEST;
				send_api_lu->header.spare1 = g_sap;
				send_api_lu->header.ver = 1;
				send_api_lu->header.len = sizeof(map_v1_update_location_request_t);

				fill_header(&p_update_loc_req_1->header,corr_id);


				fgets(line_val, MAX_LINE_BYTE, fp_apis); /* Extracting BitMap Value */
				memset(bitmap_array,'\0',20);
				strncpy(bitmap_array,line_val,20);

				while(1)
				{
					if (!fgets(line_val, MAX_LINE_BYTE, fp_apis)) {

						printf("MAP_LOCATION_UPDATE_REQUEST is successfully Parsed\n\n");
						break;
					}

					if ((*line_val == ' ') ||
							(*line_val == '#') ||
							(*line_val == '\n')) {
						continue;
					}
					else{
						//printf("Index Value is %d\n\n",atoi(bitmap_array[index]));
						//if(bitmap_array[index] == '1')
						if(1)
						{
							token_length = strtok(line_val," ");
							token_val = strtok(NULL," ");

							token_val_temp = (char *)malloc(strlen(token_val));
							strncpy(token_val_temp,token_val,strlen(token_val));

							//printf("Token Length [%s] \n",token_length);
							//printf("Token Value [%s] \n",token_val);
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
									p_update_loc_req_1->arg.imsi.length = atoi(token_length);
									p_update_loc_req_1->arg.imsi.value[counter] = hstoi(token_generic);
								}
								else if(index == 1)
								{
									p_update_loc_req_1->arg.location_info.choice = atoi(token_generic);
								}
								else if(index == 2)
								{
									p_update_loc_req_1->arg.location_info.u.v1_roaming_number.length = atoi(token_length);
									p_update_loc_req_1->arg.location_info.u.v1_roaming_number.value[counter] = hstoi(token_generic);
								}
								else if(index == 3)
								{
									//p_v2_unstructured_ss_notify_req->arg.lmsi.length  =atoi(token_length) *8;
									p_update_loc_req_1->arg.vlr_number.length  =atoi(token_length) ;
									p_update_loc_req_1->arg.vlr_number.value[counter] =hstoi(token_generic);
								}
                                else if(index == 4)
								{
									p_update_loc_req_1->arg.is_lms_id = atoi(token_generic);
								}
                                else if(index == 5)
								{
									//p_v2_unstructured_ss_notify_req->arg.lmsi.length  =atoi(token_length) *8;
									p_update_loc_req_1->arg.lms_id.length  =atoi(token_length) ;
									p_update_loc_req_1->arg.lms_id.value[counter] =hstoi(token_generic);
								}



							}

							index++;
							free(token_val_temp);

						}
						else{

							printf("No Optional Parameter is there so Please check\n");
							break;
						}

					}

				}

				fclose(fp_apis);

			}




			for(cycle_counter = 0;cycle_counter < num_cycles ; cycle_counter++ )
			{
				for(message_counter = 0; message_counter < no_mess_in_burst;message_counter++ )
				{

					//printf("Sending The message \n");
					send_api_lu_temp = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
					//map_memzero(send_api_temp,sizeof(map_api_struct_t));

					p_update_loc_req_temp_1 = (map_v1_update_location_request_t *)\
					app_mem_get(sizeof (map_v1_update_location_request_t));
				//	map_memzero(p_update_loc_req_temp, sizeof (map_update_location_request_t));



					#if 0

					if((flag_dialogue == SAME_DIALOGUE) || (flag_dialogue == SAME_DIAL_MUL_MESSAGE))
					{
						if(message_counter == 0)
						{
							corr_id = app_map_get_new_correlation_id();
							p_update_loc_req->header.corr_id =  corr_id;

							send_open_req(api_name ,corr_id);
						}
					}
					else{

					#endif
						corr_id = app_map_get_new_correlation_id();
						p_update_loc_req_1->header.corr_id =  corr_id;
						send_open_req(api_name ,corr_id);
				//	}

					memcpy(p_update_loc_req_temp_1,p_update_loc_req_1,sizeof (map_v1_update_location_request_t));

					memcpy(send_api_lu_temp,send_api_lu,sizeof(map_api_struct_t));

					p_update_loc_req_temp_1->header.invoke_id = message_counter + 1;

					//if(message_counter == (num_of_messages -1))
                    {
						p_update_loc_req_temp_1->header.last_component = 1;
					//	p_v2_unstructured_ss_notify_req_temp->header.last_component = 0;
                    }

                    //p_v2_unstructured_ss_notify_req_temp->header.corr_id =global_corr_id; 
					send_api_lu_temp->p_data = p_update_loc_req_temp_1;
					app_map_send_to_app_map((unsigned char *)send_api_lu_temp, &error);

					lu_request_counters++;

                    //send_delimiter_request(p_v2_unstructured_ss_notify_req_temp->header.corr_id);

				}

				usleep(delay);

			}

			#if 0
			app_mem_free(send_api_lu);
			app_mem_free(p_update_loc_req);

			#endif


	}


        else
        if(!strncmp(api_name,"MAP_SEND_OPEN_REQUEST",strlen("MAP_SEND_OPEN_REQUEST"))) 
        {

            
            int corr_id =142606336 ;

            corr_id = app_map_get_new_correlation_id();

            global_corr_id = corr_id;
            printf("Sending OPEN Request with Corr id [%d]\n",corr_id);
            send_open_req("MAP_PROVIDE_ROAMING_NUMBER_REQUEST",corr_id);

            send_delimiter_request(corr_id);

        }
	else 
        if(!strncmp(api_name,"MAP_PURGE_MS_REQUEST",strlen("MAP_PURGE_MS_REQUEST"))) 
	{
		printf("MAP_PURGE_MS_REQUEST Request Recieved\n");

		send_api_purgems = app_mem_get(sizeof(map_api_struct_t));
		map_memzero(send_api_purgems,sizeof(map_api_struct_t));


		p_v2_purgems_req = (map_v2_purge_ms_request_t *)\
						   app_mem_get (sizeof (map_v2_purge_ms_request_t));
				map_memzero(p_v2_purgems_req, sizeof (map_v2_purge_ms_request_t));

				fill_api_header(&(send_api_purgems->header), vlr_user_id);
				send_api_purgems->header.api_id = MAP_PURGE_MS_REQUEST;
				send_api_purgems->header.spare1 = g_sap;
				send_api_purgems->header.ver = 2;
				send_api_purgems->header.len = sizeof(map_v2_purge_ms_request_t);

				fill_header(&p_v2_purgems_req->header,corr_id);

				/* Filling the structured */

				p_v2_purgems_req->arg.imsi.length = 5;
				p_v2_purgems_req->arg.imsi.value[0] = 0x05;
				p_v2_purgems_req->arg.imsi.value[1] = 0x05;
				p_v2_purgems_req->arg.imsi.value[2] = 0x05;
				p_v2_purgems_req->arg.imsi.value[3] = 0x05;
				p_v2_purgems_req->arg.imsi.value[4] = 0x05;
				p_v2_purgems_req->arg.vlr_number.length = 5;
				p_v2_purgems_req->arg.vlr_number.value[0] = 0x05;
				p_v2_purgems_req->arg.vlr_number.value[1] = 0x05;
				p_v2_purgems_req->arg.vlr_number.value[2] = 0x05;
				p_v2_purgems_req->arg.vlr_number.value[3] = 0x05;
				p_v2_purgems_req->arg.vlr_number.value[4] = 0x05;

				/* Filling the structured */

				corr_id = app_map_get_new_correlation_id();
				p_v2_purgems_req->header.corr_id =  corr_id;
				send_open_req("MAP_PURGE_MS_REQUEST" ,corr_id);


				p_v2_purgems_req->header.invoke_id = 1;

				p_v2_purgems_req->header.last_component = 1;

				send_api_purgems->p_data = p_v2_purgems_req;
				app_map_send_to_app_map((unsigned char *)send_api_purgems, &error);

				sleep(5);

	}
	
		else{

			printf("Invalid API Hence continuing\n");
		}

	}

//	fclose(fp_apis);
	

}



void read_scenario()
{
	FILE *fp = NULL;
	char *token_api_name=NULL,*token_num_message=NULL,*token_generic = NULL,*token_dialog =NULL,*token_mul[10],*token_no_mess_burst = NULL,*token_delay=NULL;
	unsigned char line_val[MAX_LINE_BYTE];
	int line_count = 0,counter = 0,token_counter =0,api_counter = 0;
	//map_api_struct_t                   *send_api = NULL;
	//map_mo_forward_sm_request_t	*p_v1_fsm_req = NULL;

	signal (SIGUSR1,sigusr1_hdlr);
	signal (SIGUSR2, sigusr2_hdlr);

	fflush(stdout); fflush(stdin);


	//printf("Going to read app_map_user.conf file\n");



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
						token_no_mess_burst = strtok(NULL," ");
						token_delay = strtok(NULL," ");
						//token_timer_duration = strtok(NULL," ");

						p_app_map_scenario[line_count].p_app_map_apis = (app_map_apis_t *)malloc(sizeof(app_map_apis_t));

						p_app_map_scenario[line_count].is_same_dialogue = SAME_DIALOGUE;
						p_app_map_scenario[line_count].number_apis = 1;
						p_app_map_scenario[line_count].dialogue_id = INVALID_ID;

						p_app_map_scenario[line_count].p_app_map_apis[0].total_messages = atoi(token_num_message); 
						//p_app_map_scenario[line_count].p_app_map_apis[0].timer_duration = atoi(token_timer_duration); 
						//p_app_map_scenario[line_count].cycles = atoi(token_timer_duration); 
						p_app_map_scenario[line_count].p_app_map_apis[0].no_message_in_burst = atoi(token_no_mess_burst); 
						p_app_map_scenario[line_count].p_app_map_apis[0].delay_bet_burst = atoi(token_delay); 
						p_app_map_scenario[line_count].p_app_map_apis[0].count_messages = 0;
						
						strcpy(p_app_map_scenario[line_count].p_app_map_apis[0].api_name,token_api_name);

					}
					else if(!strncmp(token_dialog,"MULTIPLE_DIALOGUE",strlen("MULTIPLE_DIALOGUE")))
					{
						//p_app_map_scenario[line_count].p_app_map_apis = (app_map_apis_t *)malloc(sizeof(app_map_apis_t));

						token_api_name = strtok(NULL," ");
						token_num_message = strtok(NULL," ");
                        token_no_mess_burst = strtok(NULL," ");
						token_delay = strtok(NULL," ");

						//token_timer_duration = strtok(NULL," ");

						p_app_map_scenario[line_count].p_app_map_apis = (app_map_apis_t *)malloc(sizeof(app_map_apis_t));

						p_app_map_scenario[line_count].is_same_dialogue = MULTIPLE_DIALOGUE;
						p_app_map_scenario[line_count].number_apis = 1;

						p_app_map_scenario[line_count].p_app_map_apis[0].total_messages = atoi(token_num_message); 
						//p_app_map_scenario[line_count].p_app_map_apis[0].timer_duration = atoi(token_timer_duration); 
					//	p_app_map_scenario[line_count].cycles = atoi(token_timer_duration); 
                        p_app_map_scenario[line_count].p_app_map_apis[0].no_message_in_burst = atoi(token_no_mess_burst); 
						p_app_map_scenario[line_count].p_app_map_apis[0].delay_bet_burst = atoi(token_delay); 

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
                            token_no_mess_burst = strtok(NULL," ");
					    	token_delay = strtok(NULL," ");

							//token_timer_duration = strtok(NULL," ");


							p_app_map_scenario[line_count].p_app_map_apis[counter].total_messages = atoi(token_num_message); 
							//p_app_map_scenario[line_count].p_app_map_apis[counter].timer_duration = atoi(token_timer_duration); 
							//p_app_map_scenario[line_count].cycles = atoi(token_timer_duration); 
                             p_app_map_scenario[line_count].p_app_map_apis[counter].no_message_in_burst = atoi(token_no_mess_burst); 
					         p_app_map_scenario[line_count].p_app_map_apis[counter].delay_bet_burst = atoi(token_delay); 

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
    time(&g_initial_time);
    
    //for(;;)
    {
        for(counter = 0;counter <line_count ;counter++ )
        {


            //for(api_counter = 0; api_counter < p_app_map_scenario_send[counter].number_apis;api_counter++)
            {

                //send_open_req(corr_id);

                //for(total_mess_counter = 0;total_mess_counter < p_app_map_scenario[line_count].p_app_map_apis[api_counter].total_messages;total_mess_counter++)
                {
                    send_messages(p_app_map_scenario_send[counter].p_app_map_apis[api_counter].api_name,p_app_map_scenario_send[counter].p_app_map_apis[api_counter].total_messages,p_app_map_scenario_send[counter].is_same_dialogue,p_app_map_scenario_send[counter].p_app_map_apis[api_counter].no_message_in_burst,p_app_map_scenario_send[counter].p_app_map_apis[api_counter].delay_bet_burst);
                }


            }

        }
    }

        

}


void *thread_socket(void * arg)
{

    int sockfd,yes = 1,counter = 0,ret_val = -1;
    char msg[10];
    struct sockaddr_in dest_address;

    //dest_address.sin_family= AF_INET;
    //dest_address.sin_port=htons(2000);
    //dest_address.sin_addr.s_addr=inet_addr("172.16.107.70");
    //memset(&(dest_address.sin_zero),'\0',8);

    while(1)
    {

    sockfd = socket(AF_INET,SOCK_STREAM,0);
    if (setsockopt(sockfd,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(int)) == -1) {
		perror("setsockopt");
		exit(1);
	}
	
    strcpy(msg,"Imran");


        dest_address.sin_family= AF_INET;
        dest_address.sin_port=htons(2000);
        dest_address.sin_addr.s_addr=inet_addr("172.16.107.70");
        memset(&(dest_address.sin_zero),'\0',8);

    ret_val = connect(sockfd,(struct sockaddr *)&dest_address,sizeof(struct sockaddr));
    if(ret_val == -1)
    {
        printf("Error in Connect hence returning\n");
        sleep(2);
        continue;
    }


    for(;;)
    {

        ret_val = send(sockfd,(void *)msg,strlen(msg),0);	

        if(ret_val == -1)
        {
            printf("Error in Sending hence returning\n");
            close(sockfd);
            break;
            //return -1;
            //return SOCKET_SEND_ERROR;
        }

       sleep(5);
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
	    	signal(SIGUSR2,sigusr2_hdlr);



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
				//num_selected_fd = select (num_fd,(&map_readfd),(&map_writefd),(&map_exceptfd),&ss7p_suggested_select_timeout);
				
				if (APP_FAILURE == app_map_schedule(num_selected_fd,(&map_readfd),(&map_writefd),(&map_exceptfd), &ss7p_suggested_select_timeout, &ecode))
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
		//reg_req->spc = g_spc;
		reg_req->sap_id = sap;
		reg_req->spc = spc;

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
    	signal(SIGUSR2, sigusr2_hdlr);

	while(1)
	{

		/*Register The User */
		if((user_registered == 0) && (choice == 1))
		{

			//register_user(SMSC_SSN,msc_user_id);
			//register_user(HLR_SSN,hlr_user_id);
			//register_user(8,msc_user_id,1,1000);
			//register_user(8,msc_user_id,2,1001);
			//register_user(9,vlr_user_id,1,1000);
			//register_user(9,vlr_user_id,2,1001);
			choice = 0;
			//sleep(2);
		}
		/*User Registered */


		if(msg_to_send == 1)
		{
			read_scenario();
			msg_to_send = 0;
		}

        usleep(50000);
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


		pthread_attr_t  attr,sockattr;
		pthread_t         recvThrId;
		pthread_t         socketThrId;

		signal (SIGUSR2,sigusr2_hdlr);
		signal (SIGUSR1, sigusr1_hdlr);


		//read_scenario(); Only for testing
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

		//free(p_current_dir);
		i = 0; 
		app_map_self.port_trace = 0;
		pthread_attr_init(&attr);
		pthread_attr_setscope(&attr,PTHREAD_SCOPE_SYSTEM);
		pthread_create(&recvThrId, &attr, (void*)thread_recv, NULL);
		pthread_attr_destroy(&attr);

        /* Socket Thread */
      //  pthread_attr_init(&sockattr);
	//	pthread_attr_setscope(&sockattr,PTHREAD_SCOPE_SYSTEM);
//		pthread_create(&socketThrId, &sockattr, (void*)thread_socket, NULL);
//		pthread_attr_destroy(&sockattr);


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



void map_fill_mofsm_v3_res(map_mo_forward_sm_response_t *mo_forw_sm_res)
{ 
	static int i =0;

	mo_forw_sm_res->choice= MAP_USER_ERROR ;
	
	switch ((i++)%3)
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

	mo_forw_sm_res->response.user_error.is_parameter_present = MAP_TRUE;
	
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
		open->qos.protocol_class = MAP_SCCP_PROTOCOL_CLASS_1;
		//open->qos.protocol_class = MAP_SCCP_PROTOCOL_CLASS_0;
		open->qos.return_option = MAP_SCCP_RETURN_ON_ERROR;
		open->qos.sequence_control = 1;
		open->is_spec_info = MAP_FALSE;
		open->wait_for_delimiter = 0;
}


void fill_open_req(map_open_request_t *open, unsigned int corr_id)
{
		open->corr_id = corr_id;	
		open->is_dest_ref = 1;
		open->dest_ref.length = 7;
		open->dest_ref.value[0] = 0x1;
		open->dest_ref.value[1] = 0x1;
		open->dest_ref.value[2] = 0x1;
		open->dest_ref.value[3] = 0x1;
		open->dest_ref.value[4] = 0x1;
		open->dest_ref.value[5] = 0x1;
		open->dest_ref.value[6] = 0x1;

		open->is_orig_ref = 1;
		open->orig_ref.length = 7;
		open->orig_ref.value[0] = 0x2;
		open->orig_ref.value[1] = 0x2;
		open->orig_ref.value[2] = 0x2;
		open->orig_ref.value[3] = 0x2;
		open->orig_ref.value[4] = 0x2;
		open->orig_ref.value[5] = 0x2;
		open->orig_ref.value[6] = 0x2;
		//open->is_qos = 0;
		open->is_qos = 1;
		open->qos.protocol_class = MAP_SCCP_PROTOCOL_CLASS_1;
		//open->qos.protocol_class = MAP_SCCP_PROTOCOL_CLASS_0;
		open->qos.return_option = MAP_SCCP_RETURN_ON_ERROR;
		//open->qos.sequence_control = 1;
		open->qos.sequence_control = seq_control;
		open->is_spec_info = MAP_FALSE;
		open->wait_for_delimiter = 1;
		//open->wait_for_delimiter = 0;
		//open->wait_for_delimiter = MAP_FALSE;
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
		//open->dest_add.routing_ind = 0;
		open->dest_add.is_spc = 1;
		//open->dest_add.is_spc = 0;
		open->dest_add.spc = g_dpc;
		open->dest_add.is_ssn = 1;
		open->dest_add.ssn = a_ssn;
		open->dest_add.is_gt = 0;
		//open->dest_add.is_gt =1;
		/*Section 3.4.1 possible values = MAP_NO_GT,MAP_GT_WIT H_NAI,MAP_GT_WITH_TT,MAP_GT_WITH_TT_NP_ES,MAP_GT_WITH_TT_NP_ES_NAI,MAP_GT_RESERVED*/
		open->dest_add.global_title_ind =MAP_GT_WITH_TT_NP_ES_NAI; 
		open->dest_add.gt.type4.translation_type = 0;
		open->dest_add.gt.type4.numbering_plan = 1;
		open->dest_add.gt.type4.encoding_scheme = 2;
		open->dest_add.gt.type4.nature_of_addr_ind = 4;
		open->dest_add.gt.type4.num_gt_addr_info_octets = 255;
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
	if(last_flag)
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
	if(last_flag)
		mo_fsm_res->header.last_component = last_flag;
	
	map_fill_mofsm_v3_res(mo_fsm_res);
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
        static int count =0;
		map_api_struct_t *send_api = NULL;
		int error = 0,counter = 0,is_found = 0,api_counter = 0;

		p_api = (map_api_struct_t *)p_buffer;

		#if 0
		send_api = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
		map_memzero(send_api,sizeof(map_api_struct_t));

		fill_api_header(&(send_api->header),msc_user_id);

		#endif

		
		//signal(SIGUSR1, sigusr1_hdlr);
		//signal(SIGUSR2, sigusr2_hdlr);


	
		switch(p_api->header.api_id)
		{
				case  MAP_APP_SAP_SSN_STATE_INDICATION:
						{
								map_app_sap_ssn_state_indication_t  *open_ind;
								open_ind = (map_app_sap_ssn_state_indication_t *)p_api->p_data;
								printf("Received SAP STATE IND state [%d] SSN [%d] SAP ID [%d]\n", open_ind->state, open_ind->ssn, open_ind->sap_id);


                                if(open_ind->state == 1)
                                {
                                    //register_user(6,hlr_user_id,1,1000);
                                    register_user(7,(vlr_user_id),1,1000);
                                   // register_user(8,msc_user_id,1,1000);
                                }
                                //register_user(open_ind->ssn,msc_user_id,open_ind->sap_id,1000);
                                //register_user(open_ind->ssn,msc_user_id,open_ind->sap_id,1001);
						}
						break;

				case MAP_REGISTER_USER_CONFIRM:
						{
								printf("Received MAP_REGISTER_USER_CONFIRM \n");
								user_registered = 1;
						}
						break;
                case MAP_CHECK_IMEI_CONFIRM:
				{
                        count++;
						printf("#########Received MAP_CHECK_IMEI_CONFIRM [Count= %d ]\n",count);
				}
				break;
                case MAP_SET_REPORTING_STATE_CONFIRM:
				{
						printf("#########Received MAP_SET_REPORTING_STATE_CONFIRM \n");
				}
				break;
                case MAP_REPORT_SM_DELIVERY_STATUS_CONFIRM:
				{
						printf("#########Received MAP_REPORT_SM_DELIVERY_STATUS_CONFIRM \n");
				}
				break;
                case MAP_ALERT_SERVICE_CENTRE_CONFIRM:
				{
						printf("#########Received MAP_ALERT_SERVICE_CENTRE_CONFIRM \n");
				}
				break;
                case MAP_UNSTRUCTURED_SS_NOTIFY_CONFIRM:
				{
						printf("#########Received MAP_UNSTRUCTURED_SS_NOTIFY_CONFIRM \n");
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

								send_api = (map_api_struct_t *)app_mem_get(sizeof(map_api_struct_t));
								map_memzero(send_api,sizeof(map_api_struct_t));

								fill_api_header(&(send_api->header),msc_user_id);

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

                        
                case MAP_OPEN_CONFIRM:
                {
                        //printf("MAP_OPEN_CONFIRM Recieved #####\n");
                        break;

                }

                case MAP_CLOSE_INDICATION:
                        {
                        //printf("MAP_CLOSE_INDICATION Recieved #####\n");
                        break;

                }
				case MAP_DELIMITER_INDICATION:
						{
								map_delimiter_indication_t *delim_ind = NULL;
								if(send_api !=NULL)
								app_mem_free(send_api);
								delim_ind = (map_delimiter_indication_t *)p_api->p_data;
						}
						break;


			case MAP_MT_FORWARD_SHORT_MESSAGE_INDICATION:
			{

				map_mt_forward_sm_indication_t *mt_fsm_ind = NULL;
				mt_fsm_ind = (map_mt_forward_sm_indication_t *)p_api->p_data;


				for(counter = 0;counter < number_of_dialogues;counter++)
				{
					if(p_app_map_scenario_recv[counter].dialogue_id == mt_fsm_ind->header.dlg_id)
					{

						is_found = 1;
						break;
					}

				}

				if(!is_found)
				{

					//New Dialogue
					p_app_map_scenario_recv[number_of_dialogues].dialogue_id = mt_fsm_ind->header.dlg_id;
					number_of_dialogues++;
				}



				for(counter = 0;counter < number_of_dialogues;counter++)
				{
					if(p_app_map_scenario_recv[counter].dialogue_id == mt_fsm_ind->header.dlg_id)
					{
						for(api_counter = 0; api_counter < p_app_map_scenario_recv[counter].number_apis; api_counter++)
						{
							if(!strncmp(p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].api_name,"MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST")) && (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].total_messages > (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages - 1)))
							{
								p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages++;
								send_mt_forw_resp(p_api->header.user_id,p_api->header.ver,mt_fsm_ind->header,0);
								//send_response but not close the dialogue
								
							}
							else if(!strncmp(p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].api_name,"MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST")) && (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].total_messages == (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages - 1)))

							{
								p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages++;
								//send close request and response
								send_mt_forw_resp(p_api->header.user_id,p_api->header.ver,mt_fsm_ind->header,1);
								send_close_req(p_api->header.user_id,mt_fsm_ind->header.dlg_id,p_api->header.ver);
							}
							else{
								printf("Unknown Dialogue Id is Recived hence continuing \n");
							}
					

						}

					}
				}


		}
		break;

		case MAP_MO_FORWARD_SHORT_MESSAGE_INDICATION:
		{
			map_mo_forward_sm_indication_t *mo_fsm_ind = NULL;
			mo_fsm_ind = (map_mo_forward_sm_indication_t *)p_api->p_data;
			printf("Received MAP_MO_FORWARD_SHORT_MESSAGE_INDICATION \n"); 

			for(api_counter = 0; api_counter < p_app_map_scenario_recv[index_scenarios].number_apis; api_counter++)
			{

				if(SAME_DIALOGUE == p_app_map_scenario_recv[index_scenarios].is_same_dialogue )
				{
					if(!strncmp(p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].api_name,"MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MO_FORWARD_SHORT_MESSAGE_REQUEST")) && (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].total_messages > (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages - 1)))
					{
						p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages++;
						send_mo_forw_resp(p_api->header.user_id,p_api->header.ver,mo_fsm_ind->header,0);
						//send_response but not close the dialogue

					}
					else if(!strncmp(p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].api_name,"MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST")) && (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].total_messages == (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages - 1)))

					{
						p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages++;
						//send close request and response
						send_mo_forw_resp(p_api->header.user_id,p_api->header.ver,mo_fsm_ind->header,1);
						send_close_req(p_api->header.user_id,mo_fsm_ind->header.dlg_id,p_api->header.ver);
						index_scenarios++;
					}
					else{
						printf("Unknown Dialogue Id is Recived hence continuing \n");
					}

				}
				else if(MULTIPLE_DIALOGUE == p_app_map_scenario_recv[index_scenarios].is_same_dialogue)
				{
					if(p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].total_messages == (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages - 1))
					{
						index_scenarios++;
					}

					p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages++;
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
							send_mo_forw_resp(p_api->header.user_id,p_api->header.ver,mo_fsm_ind->header,0);
							//send_response but not close the dialogue

						}
						else if(!strncmp(p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].api_name,"MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST",strlen("MAP_MT_FORWARD_SHORT_MESSAGE_REQUEST")) && (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].total_messages == (p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages - 1)))

						{
							p_app_map_scenario_recv[counter].p_app_map_apis[api_counter].count_messages++;
							//send close request and response
							send_mo_forw_resp(p_api->header.user_id,p_api->header.ver,mo_fsm_ind->header,1);
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
			
			printf("MAP_V1_ALERT_SERVICE_CENTRE_WITHOUT_RESULT_INDICATION Recieved \n");
			 	
		}
		break;


		case MAP_MO_FORWARD_SHORT_MESSAGE_CONFIRM:
		{

			map_mo_forward_sm_confirm_t *mo_fsm_conf = (map_mo_forward_sm_confirm_t *)p_api->p_data;
			//printf("Received  MAP_MO_FORWARD_SHORT_MESSAGE_CONFIRM from MAP stack\n");


			if(mo_fsm_conf->confirm.prov_error == MAP_PROV_NO_RESPONSE_FROM_PEER)	
			{
				local_cacel_counters++;
				send_close_req(p_api->header.user_id,mo_fsm_conf->header.dlg_id,p_api->header.ver);


			}
			else
				mo_confirm_counters++;


			break;

		}
		case MAP_MT_FORWARD_SHORT_MESSAGE_CONFIRM:
		{
			
			//printf("Received  MAP_MT_FORWARD_SHORT_MESSAGE_CONFIRM from MAP stack\n");
            map_mt_forward_sm_confirm_t *mt_fsm_conf = (map_mt_forward_sm_confirm_t *)p_api->p_data;

   			if(mt_fsm_conf->confirm.prov_error == MAP_PROV_NO_RESPONSE_FROM_PEER)	
			{
				local_cacel_counters++;
				send_close_req(p_api->header.user_id,mt_fsm_conf->header.dlg_id,p_api->header.ver);


			}
			else
				mt_confirm_counters++;
		}
        break;
		case MAP_STATUS_REPORT_CONFIRM:
		{
			
			printf("Received  MAP_STATUS_REPORT_CONFIRM from MAP stack\n");
		}
        break;
		case MAP_PROVIDE_ROAMING_NUMBER_CONFIRM:
		{
			
			printf("Received  MAP_PROVIDE_ROAMING_NUMBER_CONFIRM from MAP stack\n");
		}
        break;
        case MAP_PROCESS_UNSTRUCTURED_SS_REQUEST_CONFIRM:
		{
			#if 0
			map_v2_process_unstructured_ss_request_confirm_t *ussr_confirm = (map_v2_process_unstructured_ss_request_confirm_t *)p_api->p_data;
			

			if(ussr_confirm->confirm.prov_error == MAP_PROV_NO_RESPONSE_FROM_PEER)	
			{
				local_cacel_counters++;
				send_close_req(p_api->header.user_id,mt_fsm_conf->header.dlg_id,p_api->header.ver);

			}
				
			#endif

				
			//printf("Received  MAP_PROCESS_UNSTRUCTURED_SS_REQUEST_CONFIRM from MAP stack\n");
		}
		break;
        case MAP_UNSTRUCTURED_SS_REQUEST_CONFIRM:
		{
			
			//printf("Received  MAP_UNSTRUCTURED_SS_REQUEST_CONFIRM from MAP stack\n");
			map_v2_unstructured_ss_request_confirm_t *ussr_confirm = (map_v2_unstructured_ss_request_confirm_t *)p_api->p_data;
			
			//printf("Response recevied [%d]\n",ussr_confirm->confirm.prov_error);

			if(ussr_confirm->confirm.prov_error == MAP_PROV_NO_RESPONSE_FROM_PEER)	
			{
				local_cacel_counters++;
				send_close_req(p_api->header.user_id,ussr_confirm->header.dlg_id,p_api->header.ver);


			}
			else
				ussd_confirm_counters++;
		}
		break;

        case MAP_SEND_ROUTING_INFO_FOR_SM_CONFIRM:
        {
		
		map_absent_subscriber_error_t *absent_subs=NULL;
		map_routing_info_for_sm_confirm_t *ussr_confirm = (map_routing_info_for_sm_confirm_t *)p_api->p_data;

		if(ussr_confirm->confirm.prov_error == MAP_PROV_NO_RESPONSE_FROM_PEER)	
		{
			local_cacel_counters++;
			send_close_req(p_api->header.user_id,ussr_confirm->header.dlg_id,p_api->header.ver);


		}
		else
		{
			absent_subs = (map_absent_subscriber_error_t *) &(ussr_confirm->confirm.user_error.user_error.absent_sub);
			printf("Error Code is  [%d]\n",absent_subs->choice);
			printf("Value  is  [%d]\n",absent_subs->u.v2_absent_sub);
			srism_confirm_counters++;
		}
            
        }
        break;
	case MAP_SEND_ROUTING_INFORMATION_CONFIRM:
        {

		map_send_routing_info_confirm_t *ussr_confirm = (map_send_routing_info_confirm_t *)p_api->p_data;

		if(ussr_confirm->confirm.prov_error == MAP_PROV_NO_RESPONSE_FROM_PEER)	
		{
			local_cacel_counters++;
			send_close_req(p_api->header.user_id,ussr_confirm->header.dlg_id,p_api->header.ver);


		}
		else
			sri_confirm_counters++;


        }
        break;

        case MAP_REMOTE_USER_FREE_CONFIRM:
        {
            printf("Received  MAP_REMOTE_USER_FREE_CONFIRM from MAP stack\n");

        }
        break;
        case MAP_IST_ALERT_CONFIRM:
        {
            printf("Received MAP_IST_ALERT_CONFIRM from MAP stack\n");

        }
        break;
        case MAP_IST_COMMAND_CONFIRM:
        {
            printf("Received MAP_IST_COMMAND_CONFIRM from MAP stack\n");

        }
        break;

        case MAP_RESTORE_DATA_CONFIRM:
        {
            printf("Received MAP_RESTORE_DATA_CONFIRM from MAP stack\n");

        }
        break;
        case MAP_INSERT_SUBSCRIBER_DATA_CONFIRM:
        {
            printf("Received MAP_INSERT_SUBSCRIBER_DATA_CONFIRM from MAP stack\n");

        }
        break;
	case MAP_UPDATE_LOCATION_CONFIRM:
	{

		map_update_location_confirm_t *ussr_confirm = (map_update_location_confirm_t *)p_api->p_data;

		if(ussr_confirm->confirm.prov_error == MAP_PROV_NO_RESPONSE_FROM_PEER)	
		{
			local_cacel_counters++;
			send_close_req(p_api->header.user_id,ussr_confirm->header.dlg_id,p_api->header.ver);


		}
		else
			lu_confirm_counters++;
	}
	break;

	case MAP_PURGE_MS_CONFIRM:
	{
		printf("MAP_PURGE_MS_CONFIRM Confirmation Recieved \n");
	}
	break;
        case MAP_INFORM_SERVICE_CENTRE_INDICATION:
		{
			printf("MAP_INFORM_SERVICE_CENTRE_INDICATION Indication recieved\n");
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


				default:
						printf("SOME OTHER API\n");
						break;

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

