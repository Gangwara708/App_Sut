/****************************************************************************
 * FILE: app_map_main.c         
 ****************************************************************************
 *
 * DESCRIPTION:
 *      This file contains main function for app map port / user lib.
 *
 * DATE         NAME            SPR     REASON
 * ------------ --------------- ------- --------------------------------------
 * Jun 13 2005  Gaurav Sharma           Initial Coding 
 * Sep 2 2005	swapnil			SPR-16310
 * Oct 22 2005  Gaurav Sharma 	 16426	Schedule returning error
 * Feb 01 2007  Saurabh Thukral SPR-17456      
 * Mar 16 2007  Saurabh Thukral SPR-17456 Patch Fix      
 * Apr 10 2008  Saurabh Thukral SPR-18532 Klocworks fixes      
 * Jan 29 2010  Srinivasu Ryali           NexGen Diagnostic(1.3.0.0.0)
 * Jan 30 2010  sthukral                  1_3_0: client ports  
 * Jan 25 2011  Vivek Gupta               CSR Fix 1-9318794 	
 * Mar 07 2011  Vivek Gupta 			SPR 20519	
 * Jul 10 2011  Ritesh Kumar  		  CSR Fix 00002923
 * Sep 15 2011  Aakash Roy        SPR 20843   Distribution towards TEP using user ported functions and dll
 * Sep 22 2011  Asish       			SPR 20859		
 * Jan 16 2012  Vinay Upreti      SPR 21020		
 * Dec 05 2012  Imran Lateef			SPR-21313
 * Dec 05 2012  Imran Lateef			SPR-21327
 * Jan 21 2013  Ritvik Sahai			SPR-21354 Fix Socket Buffer made Configurable
 * Jul 04 2013  Supreet Jain    21474   Get app_map user lib version 
 * Dec 02 2013  Sivaraj Pillai    21539   Statistic Collection 
 * Copyright (c) 2005, Flextronics Software Systems Ltd. All rights reserved.
 * No part of this source code may be reproduced or transmitted in any form or 
 * by any means, electronic or otherwise, including photocopying, reprinting, 
 * or recording, for any purpose, without the express written permission of 
 * Flextronics Software Systems Ltd.
 ****************************************************************************/
#ifndef APP_DYN_LIBRARY
#include "ss7p_user.h"
#include "ss7p.h"
#endif
#include "app_map_main.h"
#ifdef APPL_OS_SOLARIS
#include <ucontext.h>
#endif
#ifdef APPL_OS_LINUX
#include <execinfo.h>
#endif

#include <nw_util.h> /*SPR-21354 Fix*/
/* SPR 20843 Starts */
#include <dlfcn.h>
/* SPR 20843 Ends */

app_start_param_t 		app_map_start_param; 
app_S8bit_t    			app_map_gb_dirname[400];
app_S8bit_t 			*p_app_map_current_dir;
/* SPR-17456 Starts */
/* The application platform mode.Its values can be APP_MAP_DISTRIBUTED_MODE or
 * APP_MAP_DEDICATED_MODE. Default APP_MAP_DISTRIBUTED_MODE */
app_U8bit_t app_mode = APP_MAP_DISTRIBUTED_MODE;
/* SPR-17456 Ends */
/*Start Declearation of variable of Exit Handler which handle the generation of core while calling exit handler*/ 
/*CSR:00002923 Fix Start*/
unsigned short g_enable_exit_handler = APP_FALSE;
/*CSR:00002923 Fix End*/
/*End Declearation of variable of Exit Handler which handle the generation of core while calling exit handler*/ 

/*SPR-21354 Fix Starts*/
/*amir changes S*/
unsigned char g_syslog_to_be_used=APP_TRUE;
char my_process_id[20];
/*amir changes E*/
extern unsigned int g_app_map_send_buff = APPL_SOCK_BUF_SIZE; 
extern unsigned int g_app_map_recv_buff = APPL_SOCK_BUF_SIZE;
unsigned int g_app_map_soc_mgr_send_buff = SOC_MGR_MAX_BUF;
unsigned int g_app_map_soc_mgr_recv_buff = SOC_MGR_MAX_BUF;
#ifdef APP_DYN_LIBRARY
unsigned int user_send_sock_buff = 0;
unsigned int user_recv_sock_buff = 0;
void app_map_user_buffer_request(unsigned int , unsigned int );
#endif

/*SPR-21354 Fix Ends*/
/* SPR#21539 start */
extern U32bit	app_map_api_lib_stat_table[MAP_MAX_APIS][APPL_MAX_DESTINATIONS];
/* SPR#21539 end  */

/*SPR-21313 Fix Starts Here*/
int max_fd_val=0;
#define APP_SET_MAX_FD(fd) max_fd_val=(fd>max_fd_val)?fd:max_fd_val;
int sig_usr2_log = 0;
/*SPR-21313 Fix Ends Here*/
#ifdef APP_DYN_LIBRARY
app_U8bit_t app_map_dmr_process_user_mesg
#ifdef ANSI_PROTO
    (app_U8bit_t        *p_buffer, app_errs_t         *p_ecode);
#else
	();
#endif
#ifdef APP_MAP_MT_SAFE
extern QLOCK		app_map_user_info_lock;
extern QLOCK		app_map_last_inst_lock;
/* SPR-17456 Starts */
/* Not required */
/*extern QLOCK		app_map_corr_id_lock;*/
/* SPR-17456 Ends */
extern QLOCK		app_map_remote_client_conn_lock;
/* SPR#21539 start */
QLOCK                       app_map_user_stat_lock;
/* SPR#21539 end */
#endif
#else
app_U8bit_t				app_map_num_active_fep_list[256];
app_U8bit_t				app_map_num_active_fep_count = 0;
app_U8bit_t        		app_tcap_instance_status = APP_INSTANCE_UNCONFIG;
app_U8bit_t				app_map_bulk_i_am_active_sent = APP_FALSE;
#endif
void exit_handler(void);
void inst_sig_hdlr (void);
void signal_log_n_ignore(int sig);
void signal_log_n_exit(int sig);
void log_signal(int sig);

/*SPR-21313 Fix Starts Here*/
#ifndef APP_DYN_LIBRARY
extern void ss7p_toggle_hex_dumping();
#endif
app_U8bit_t map_glb_proto_trace_en = 0;
/*SPR-21313 Fix Ends Here*/
void log_signal(int sig)
{
    char filename[100];
    time_t		 	sec  = 0;
    time_t		 	nsec = 0;
    char 				start_string[200]={0};
    app_logger_get_curr_time( &sec, &nsec );
    app_logger_get_date(sec, nsec, start_string);


	switch (sig)
	{
			case SIGTSTP:
		{
				
				SPL_TRACE(("SIGNAL Recevied: SIGTSTP\n"));
		}
			    break;	
			case SIGUSR1:
		{
				SPL_TRACE(("SIGNAL Recevied: SIGUSR1\n"));
			/*SPR-21313 Fix Starts Here*/
				if(0 == map_glb_proto_trace_en)
				{
					map_glb_proto_trace_en = 1;
					app_map_tm_gbl.state = LOGGING_TO_BE_DONE;
				}
				else
				{
					map_glb_proto_trace_en = 0;
			  }
			/*SPR-21313 Fix Ends Here*/

			}
			break;	
		case SIGUSR2:
			{
			/*SPR-21313 Fix Starts Here*/	
				/*MAP_PRINT_PROTO_STATS()*/
				sig_usr2_log = 1;
			/*SPR-21313 Fix Ends Here*/	
				SPL_TRACE(("SIGNAL Recevied: SIGUSR2\n"));
		}
			    break;	
			case SIGHUP:
		{
				SPL_TRACE(("SIGNAL Recevied: SIGHUP\n"));
		}
			    break;	
			case SIGWINCH:
		{
			SPL_TRACE(("SIGNAL Recevied: SIGWINCH\n"));
		}
			    break;	
			case SIGPIPE:
		{
			SPL_TRACE(("SIGNAL Recevied: SIGPIPE\n"));
		}
			    break;	
			case SIGTERM:
		{
			SPL_TRACE(("SIGNAL Recevied: SIGTERM\n"));
		}		
			    break;	
            case SIGKILL:
		{
			SPL_TRACE(("SIGNAL Recevied: SIGKILL\n"));
		}
			    break;
			default:
		{
			SPL_TRACE((" Unhandled signal [%d]\n",sig));
		}			   
			    break;
	}	
	return;	
}

void signal_log_n_ignore(int sig)
{
    log_signal(sig);
    return;	

}

void signal_log_n_exit(int sig)
{
	log_signal(sig);
	exit_handler();
	return;
}


void exit_handler(void)
{

#ifdef APPL_OS_LINUX

#define BT_SIZE     100
#define STR_SIZE    200
    int j, nptrs;

    void *buffer[BT_SIZE];
    char **strings;
    time_t		 	sec  = 0;
    time_t		 	nsec = 0;
    char 				start_string[STR_SIZE]={0};
    app_logger_get_curr_time( &sec, &nsec );
    app_logger_get_date(sec, nsec, start_string);

    nptrs = backtrace(buffer, BT_SIZE);
    SPL_TRACE(("[%s] Process Exiting:::  current call-stack has [%d] addresses\n",start_string, nptrs));

    strings = backtrace_symbols(buffer, nptrs);
    if (strings == NULL)
    {
       SPL_TRACE(("backtrace_symbols system call returned NULL\n"));
	   abort();
    }

    for (j = 0; j < nptrs; j++)
    {
         if(strings[j]!=NULL)
         {
             printf("Frame[%d] %s\n",j,strings[j]);
             SPL_TRACE(("[%s] Frame[%d]\n",strings[j], j));
         }
        else
        {
            SPL_TRACE(("backtrace_symbols system call returned NULL for valid stack frame %d\n", j));
        }

    }
    free(strings);
#endif

#ifdef APPL_OS_SOLARIS
    printstack(fileno(gbl_spl_trace_fp));
#endif
/*CSR:00002923 Fix Start*/
if( g_enable_exit_handler == APP_TRUE)
{
	SPL_TRACE(("Forcing To Dump Core\n"));
#ifndef WINNT	
    abort();
#endif
}
else
exit(0);
/*CSR:00002923 Fix Start*/
}

    

void inst_sig_hdlr (void)
{	
	struct sigaction sig;
	sig.sa_handler = signal_log_n_ignore;
	sigemptyset(&sig.sa_mask);
	sig.sa_flags = 0;
	sigaction(SIGTSTP,&sig,NULL);
	sigaction(SIGUSR2,&sig,NULL);
	sigaction(SIGHUP,&sig,NULL);
	sigaction(SIGWINCH,&sig,NULL);
	sigaction(SIGUSR1,&sig,NULL);
	
	sig.sa_handler = SIG_DFL;
	sigaction(SIGALRM,&sig,NULL);
	
	sig.sa_handler = SIG_IGN;
	sigaction(SIGTTOU,&sig,NULL);
	sigaction(SIGTTIN,&sig,NULL);
	sigaction(SIGPIPE,&sig,NULL);
	
 	sig.sa_handler = signal_log_n_exit;
	sigaction(SIGTERM,&sig,NULL);
    sigaction(SIGKILL,&sig,NULL);

	atexit(exit_handler);
}

/****************************************************************************
** Function: main / app_map_init
****************************************************************************
** DESCRIPTION: This is a main function 
** 
** Usage: app_map -t tier -i inst_id -a ip:port [-s ip:port] -m ip 
** 			-h	help
** 			-t	tier level of APP platform process
** 			-i	instance id in the tier
** 			-a	ip address & port of active em
** 			-s	ip address & port of standby em
** 			-m	machines's ip address on which this process runs. Client port to connect to EM 
**              is optional
**			-d  directory where logging of this instance is to be done 
**
**
**
** PARAMETERS :
** 			tier			: If this is MAP or MAP-DMR 
**									1 for MAP, 2 for MAP-DMR 
**			instance_id 	: instance_id for fep / bep
**			actv_em_ip		: IP Address for actv EM 
**			actv_em_port	: Port for actv EM
**			stdby_em_ip 	: IP Address for standby EM
**			stdby_em_port	: Port for standby EM
**          self_ip   		: Self IP address (useful for floating ip)
**          ipc_opt         : APPL_IPC_OPT_USE_SELECT=> we have to call select
**                            APPL_IPC_OPT_NO_SELECT=> we do not have to 
**                                                     call select              
**			
** RETURN VALUE : 
**         APP_FAILURE - 0 on failure
**		   APP_SUCCESS - 1 on success
***************************************************************************/

#ifdef APP_DYN_LIBRARY
int	app_map_init
#ifdef ANSI_PROTO
	(unsigned int 		tier,
	unsigned int 		inst_id, 
	unsigned char 		*p_a_ep_m_ip, 
	unsigned int 		a_em_port, 
	unsigned char 		*p_s_ep_m_ip, 
	unsigned int 		s_em_port, 
	unsigned char 		*p_m_ip,
	unsigned int 		ipc_opt,
	unsigned char   	*p_dirname,
	unsigned int 		*p_ecode)
#else
	(tier, inst_id, p_a_ep_m_ip, a_em_port, p_s_ep_m_ip, 
	s_em_port, p_m_ip,	ipc_opt, p_dirname, p_ecode)
	unsigned int 		tier;
	unsigned int 		inst_id; 
	unsigned char 		*p_a_ep_m_ip; 
	unsigned int 		a_em_port; 
	unsigned char 		*p_s_ep_m_ip; 
	unsigned int 		s_em_port; 
	unsigned char 		*p_m_ip;
	unsigned int 		ipc_opt;
	unsigned char   	*p_dirname;
	unsigned int 		*p_ecode;
#endif
#else /* APP_DYN_LIBRARY */
int	main
#ifdef ANSI_PROTO
	(int C, char **V )
#else
	(C,V )
 	int C;
	char **V;
#endif
#endif
{
	int 				retval ; 
 	struct timeval 		T;
    struct sigaction 	new_action;
	app_S8bit_t			file_name[30];
	/*amir changes S*/
	pid_t my_proc_id;
	/*amir changes E*/
/* SPR 20859 Fix Start*/
	char		bin_file_name[512];  
/* SPR 20859 Fix End*/

#ifndef APP_DYN_LIBRARY
	int 				error = APP_NULL ; 
    /* Changes for 1.3.0.0.0_RSy-1 Start */
    app_U8bit_t ss7p_ip_str[APPL_MAX_IPADDR_LEN+10];
    /* Changes for 1.3.0.0.0_RSy-1 End */
#ifndef APPL_OS_LINUX
/*SPR#21020 fix starts*/
/*	struct sockaddr_in  ina; 
	unsigned int		x=0;
	struct sockaddr_in  ina; */
/*SPR#21020 fix ends*/
#endif
#endif

	qvSimpleInit(&app_map_shell_os); 
	app_map_tm_early_init();
    inst_sig_hdlr();
#ifdef APP_DYN_LIBRARY
	*p_ecode = APPL_ERR_NO_ERROR; 
	/*amir changes S*/
	if(ipc_opt & APPL_OPT_SYSLOG_OPT)
	{
		g_syslog_to_be_used = APP_FALSE;
		ipc_opt &= (~APPL_OPT_SYSLOG_OPT);
	}
	/*amir changes E*/
 
    if ((ipc_opt!=APPL_IPC_OPT_USE_SELECT)
			 && (ipc_opt!=APPL_IPC_OPT_NO_SELECT))
	{
        APPL_PORT_TRACE(("APPL PORT [MAP] :: ERROR : INVALID Scheduling option\n"));
		*p_ecode = APPL_ERR_INVALID_IPC_OPT; 
        return (APP_FAILURE);
    }

    app_map_ipc_opt = ipc_opt;
#else
    app_map_ipc_opt = APPL_IPC_OPT_USE_SELECT;
#endif

	gettimeofday( &T, 0 );
	app_map_local_time =  T.tv_sec;
	
	
	app_map_port_init_datastructures();

#ifdef APP_DYN_LIBRARY

	if (app_map_validate_n_store_start_param(tier,inst_id,p_a_ep_m_ip,a_em_port,
			p_s_ep_m_ip,s_em_port,
			p_m_ip , p_dirname) == 0)
	{
		*p_ecode = APPL_ERR_INVALID_INPUT_PARAM; 
		return (APP_FAILURE);
	}
#else
	if ( app_map_validate_n_store_start_param(C, V) == 0 )
	{
	APPL_EXIT(APP_FAILURE, APPL_ERR_INVALID_INPUT_PARAM);
	}

/*amir changes S*/
#ifdef APP_DYN_LIBRARY
	if(g_syslog_to_be_used)
	{
		if(SS7P_SUCCESS == tm_is_syslog_running())
		{
			my_proc_id=getpid();
			strncpy(my_process_id,"TEP:",strlen("TEP:"));
			snprintf(my_process_id+(strlen(my_process_id)),10,"%d",inst_id);
			snprintf(my_process_id+(strlen(my_process_id)),10,"%d",my_proc_id);
			openlog(my_process_id, LOG_NDELAY, LOG_LOCAL1);
			openlog(my_process_id, LOG_NDELAY, LOG_LOCAL2);
			openlog(my_process_id, LOG_NDELAY, LOG_LOCAL3);
		}
		else
		{
			fprintf(stderr, "TEP :: SYSLOG NOT RUNNING\n");
			g_syslog_to_be_used=SS7P_FALSE;
		}


	}
#else
	if(g_syslog_to_be_used)
	{
		if(SS7P_SUCCESS == tm_is_syslog_running())
		{

			my_proc_id=getpid();
			strncpy(my_process_id,"BEP:",strlen("BEP:"));
			strcat(my_process_id,"<");
			snprintf(my_process_id+(strlen(my_process_id)),10,"%d",app_map_self.instance_id);
			strcat(my_process_id,">");
			snprintf(my_process_id+(strlen(my_process_id)),10,"%d",my_proc_id);
			openlog(my_process_id, LOG_NDELAY, LOG_LOCAL1);
			openlog(my_process_id, LOG_NDELAY, LOG_LOCAL2);
			openlog(my_process_id, LOG_NDELAY, LOG_LOCAL3);
		}
		else
		{
			fprintf(stderr, "BEP :: SYSLOG NOT RUNNING\n");
			g_syslog_to_be_used=SS7P_FALSE;
		}

	}
#endif

	/*amir changes E*/





    /* SPR 20859 Fix Start */
	if(app_map_self.tier == TIER_2_VAL)
	{	
		strcpy(bin_file_name,V[0]);	
		if (app_logger_intialize_exclusive_logging(bin_file_name) == APP_SUCCESS)
		{
			printf("APP PORT [MAP] :: Exclusive Logging Initialized Successfully\n");
		} 
		else
		{
			printf("APP PORT [MAP] :: Exclusive Logging Not Initialized \n");
		}
	}
	/* SPR 20859 Fix End */

#endif
	if(app_map_self.tier == TIER_3_VAL)
	{
		sprintf(file_name,"LOGS_Map_Dmr_Inst_%d", app_map_self.instance_id);	
	}
	else if(app_map_self.tier == TIER_2_VAL)
	{	
		sprintf(file_name,"LOGS_Map_Inst_%d", app_map_self.instance_id);
	}
	else
	{
		/* SPR-18532 Starts */
		snprintf(file_name,sizeof(file_name),"LOGS_Map_Tier_%d_Inst_%d",
			app_map_self.tier, app_map_self.instance_id);
		/* SPR-18532 Ends */
	}
	retval = app_map_tm_init(file_name,app_map_gb_dirname);

#ifdef APP_DYN_LIBRARY
	if (retval == APP_FAILURE)
	{
    	printf("APP PORT [MAP] :: ERROR :Error in initialising logging module\n");
		*p_ecode = APPL_ERR_IN_LOG_INIT; 
		return APP_FAILURE;
	}
#else
	if (retval == APP_FAILURE)
	{
    	APPL_EXIT(APP_FAILURE, APPL_ERR_IN_LOG_INIT);
	}
#endif

	/* establish IPC with active/Standby EM */
	qvOpen(&app_map_shell, NULL);
	/*1.3.0.0.0_Rf1 start */
	log_util_create_spl_log_file();
	/*1.3.0.0.0_Rf1 End */
#ifndef APP_DYN_LIBRARY

#ifndef APPL_OS_LINUX
/*SPR#21020 fix starts*/
    /* Changes for 1.3.0.0.0_RSy-1 Start */
    if(app_map_self.client_authenticate)
        snprintf(ss7p_ip_str,sizeof(ss7p_ip_str),"%s:%d",app_map_start_param.self_ip,
                   app_map_start_param.self_client_port);
    else
        snprintf(ss7p_ip_str,sizeof(ss7p_ip_str),"%s",app_map_start_param.self_ip);
/*SPR#21020 fix ends*/
#else
    if(app_map_self.client_authenticate)
        snprintf(ss7p_ip_str,sizeof(ss7p_ip_str),"%s:%d",app_map_start_param.self_ip,
                   app_map_start_param.self_client_port);
    else
        snprintf(ss7p_ip_str,sizeof(ss7p_ip_str),"%s",app_map_start_param.self_ip);

#endif
    /* Changes for 1.3.0.0.0_RSy-1 End */

    if (ss7p_init(  2 /* BEP */ ,app_map_self.instance_id, 
					app_map_entity_info[1].ipaddr,
					app_map_entity_info[1].port - APP_MAP_EM_PORT_OFFSET,
                    app_map_entity_info[2].ipaddr,
					app_map_entity_info[2].port - APP_MAP_EM_PORT_OFFSET,
/* Changes for 1.3.0.0.0_RSy-1 Start */
                    ss7p_ip_str,
/* Changes for 1.3.0.0.0_RSy-1 End */
                    SS7P_IPC_OPT_NO_SELECT,
					p_app_map_current_dir,
					&error) == 0)
    {
        APPL_PORT_TRACE(("Could not initialise TMAP \n")); 
    	APPL_EXIT(APP_FAILURE, APPL_ERR_IN_EM_MODULE_REGISTER);
    }
#endif

	/* registering cspl module for em agent */
	qvRegisterEx(0,0,0, &app_map_em_agent_stack_manifest, NULL, 
		QV_TPOOL_EXT, &retval );

#ifdef APPL_LICENSE_INFO_ENABLED
		qvRegisterEx(0,0,0, &app_map_license_stack_manifest, NULL, 
		QV_TPOOL_EXT, &retval );
#endif
	
	/* making Active EM as cspl destination */
	app_map_entity_info[1].entity_id = APP_EM1_ENTITY_ID;
	app_map_entity_info[1].cspl_service = APP_MAP_EM1_SERV_ID;
	qvSetDestination (APP_MAP_EM1_SERV_ID, 
			&app_map_shell, NULL, NULL );
	
	/* making Standby EM as cspl destination */
	app_map_entity_info[2].entity_id = APP_EM2_ENTITY_ID;
	app_map_entity_info[2].cspl_service = APP_MAP_EM2_SERV_ID;
	qvSetDestination (APP_MAP_EM2_SERV_ID, 
			&app_map_shell, NULL, NULL );
	/* remaining cspl modules and destinations shall be added on obtaining
	   response for APPL_IMUP message to EM */

	if ( retval != QVERROR_NONE )
	{
		APPL_PORT_TRACE(("APPL PORT [MAP] :: error %d in registering %s\n", 
			retval, app_map_em_agent_stack_manifest.name ));
#ifdef APP_DYN_LIBRARY
		*p_ecode = APPL_ERR_IN_EM_MODULE_REGISTER; 
		return APP_FAILURE;	
#else
    	APPL_EXIT(APP_FAILURE, APPL_ERR_IN_EM_MODULE_REGISTER);
#endif
	}

    new_action.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &new_action, NULL);
#ifndef APP_DYN_LIBRARY
/* SPR 20843 Starts */
	app_map_user_port_umr_initialize();
/* SPR 20843 Ends */

/* SPR 20843 Starts */
	if(app_map_user_port_umr_global.dll_app_map_user_port_umr_init != APP_NULL)
	{
			app_map_user_port_umr_global.dll_app_map_user_port_umr_init(); 
			APPL_PORT_TRACE(("APP MAP [MAIN] :: SUCCESSFUL IN CALLING DLL FUNC app_map_user_port_umr_init\n"));
			SPL_TRACE(("APP MAP [MAIN] :: SUCCESSFUL IN CALLING DLL FUNC app_map_user_port_umr_init\n"));
	}
	else
	{
			APPL_PORT_TRACE(("APP MAP [MAIN] :: DLL FUNC app_map_user_port_umr_init HAS NOT BEEN LOADED and hence could not be called.\n"));
			SPL_TRACE(("APP MAP [MAIN] :: DLL FUNC app_map_user_port_umr_init HAS NOT BEEN LOADED and hence could not be called.\n"));
	}
/* SPR 20843 Ends */
#endif
    
#ifndef APP_DYN_LIBRARY
	qvRun(&app_map_shell, &app_map_listener, &app_map_shell_waitstruct, 0); 
	/* Control never reaches here*/
#endif

#ifdef APP_DYN_LIBRARY
#ifdef APP_MAP_MT_SAFE
    app_map_user_info_lock = qvNewLock();
    app_map_last_inst_lock = qvNewLock();
/* SPR-17456 Starts */
/* Not required */
/*        app_map_corr_id_lock = qvNewLock(); */

/* SPR-17456 Ends */
    app_map_remote_client_conn_lock = qvNewLock();
/* SPR#21539 start */
    app_map_user_stat_lock = qvNewLock();
    APP_MAP_UT_ASSERT(app_map_user_stat_lock!=APP_NULL);	
/* SPR#21539 end */
#endif
#endif
	return APP_SUCCESS;
}

/*CSR:00002923 Fix Start*/
/****************************************************************************
** app_map_init_ext
****************************************************************************
** DESCRIPTION: This is a main function 
** 
** Usage: app_map -t tier -i inst_id -a ip:port [-s ip:port] -m ip 
** 			-h	help
** 			-t	tier level of APP platform process
** 			-i	instance id in the tier
** 			-a	ip address & port of active em
** 			-s	ip address & port of standby em
** 			-m	machines's ip address on which this process runs. Client port to connect to EM 
**              is optional
**			-d  directory where logging of this instance is to be done 
**
**
**
** PARAMETERS :
** 			tier			: If this is MAP or MAP-DMR 
**									1 for MAP, 2 for MAP-DMR 
**			instance_id 	: instance_id for fep / bep
**			actv_em_ip		: IP Address for actv EM 
**			actv_em_port	: Port for actv EM
**			stdby_em_ip 	: IP Address for standby EM
**			stdby_em_port	: Port for standby EM
**          self_ip   		: Self IP address (useful for floating ip)
**          ipc_opt         : APPL_IPC_OPT_USE_SELECT=> we have to call select
**                            APPL_IPC_OPT_NO_SELECT=> we do not have to 
**                                                     call select              
**			
** RETURN VALUE : 
**         APP_FAILURE - 0 on failure
**		   APP_SUCCESS - 1 on success
***************************************************************************/

#ifdef APP_DYN_LIBRARY
int	app_map_init_ext
#ifdef ANSI_PROTO
	(unsigned int 		tier,
	unsigned int 		inst_id, 
	unsigned char 		*p_a_ep_m_ip, 
	unsigned int 		a_em_port, 
	unsigned char 		*p_s_ep_m_ip, 
	unsigned int 		s_em_port, 
	unsigned char 		*p_m_ip,
	unsigned int 		ipc_opt,
	unsigned char   	*p_dirname,
	unsigned int		exit_flag,
	unsigned int 		*p_ecode)
#else
	(tier, inst_id, p_a_ep_m_ip, a_em_port, p_s_ep_m_ip, 
	s_em_port, p_m_ip,	ipc_opt, p_dirname, exit_flag, p_ecode)
	unsigned int 		tier;
	unsigned int 		inst_id; 
	unsigned char 		*p_a_ep_m_ip; 
	unsigned int 		a_em_port; 
	unsigned char 		*p_s_ep_m_ip; 
	unsigned int 		s_em_port; 
	unsigned char 		*p_m_ip;
	unsigned int 		ipc_opt;
	unsigned char   	*p_dirname;
	unsigned int		exit_flag;
	unsigned int 		*p_ecode;
#endif
{
	if(exit_flag != APP_FALSE)
	{
		g_enable_exit_handler = APP_TRUE;
	}
	return app_map_init(tier, inst_id, p_a_ep_m_ip, a_em_port, p_s_ep_m_ip, 
			s_em_port, p_m_ip, ipc_opt, p_dirname, p_ecode);
}
#endif
/*CSR:00002923 Fix End*/


/****************************************************************************
** Function: app_map_schedule 
**
****************************************************************************
** DESCRIPTION: This is the scheduler function of APPL that needs to be 
** called in the control loop (the loop where USER is checking for all the 
** messages/responses/timeouts etc). This function is a non-blocking function
** and it shall be 
** called periodically to schedule APPL instance. These function checks for 
** any messages from different APPL Interfaces (like EM) and it also 
** checks for timeout messages of APPL started timers.
** RETURN:
**       APP_FAILURE 0 on failure, ecode contains error code
**       APP_SUCCESS 1 on success.
**
***************************************************************************/
#ifdef APP_DYN_LIBRARY
int app_map_schedule
#ifdef ANSI_PROTO
		( int num_fd, 
		  fd_set *p_readfds,
          fd_set *p_writefds,
          fd_set *p_excepfds,
          struct timeval *p_user_timeout,
   		  unsigned int *p_ecode)
#else
		( num_fd, p_readfds,p_writefds,
          p_excepfds, p_user_timeout, p_ecode)
		  int 			num_fd; 
		  fd_set 		*p_readfds;
          fd_set 		*p_writefds;
          fd_set 		*p_excepfds;
          struct timeval *p_user_timeout;
   		  unsigned int 	*p_ecode;
#endif
{
	const QSHELL 	*S; 
	void 			*listener; 
	const QWAIT 	*W; 
	int 			policy;
    struct timeval 	my_timeout;  
    QTIME 			user_time, my_time;
    QTIME 			now, diff;
	int 			retval; 
	int 			max_fd = FD_SETSIZE;

	*p_ecode = APPL_ERR_NO_ERROR; 
    S = &app_map_shell; 
	W = &app_map_shell_waitstruct;
	policy = 0;

    qvAssert( S && S->timedwait );

    if (app_map_ipc_opt == APPL_IPC_OPT_USE_SELECT) 
    {
			FD_ZERO(&app_map_listener.readfds);
			FD_ZERO(&app_map_listener.writefds);
			FD_ZERO(&app_map_listener.excepfds);

        retval = app_map_pre_select(&max_fd, 
				&app_map_listener.readfds, 
				&app_map_listener.writefds,
                &app_map_listener.excepfds, 
				&my_timeout, p_ecode);
		
		if ( retval == APP_FAILURE )
		{
			return retval; /* note p_ecode is set by app_map_pre_select */
		}
		
        /* now compare p_user_timeout & my_timeout, choose the minimum of these */
        if(p_user_timeout) 
		{
            user_time.s= p_user_timeout->tv_sec;
            user_time.us = p_user_timeout->tv_usec;
        } 
		else 
		{
            user_time.s= 3;
            user_time.us = 0;
        }
        my_time.s= my_timeout.tv_sec; 
        my_time.us = my_timeout.tv_usec;

		
        if( qvTimeDiff(&user_time, &my_time, &diff) < 0 ) 
        {
            /* select() will be called by us */
            S->timedwait(&app_map_listener, &user_time);
        }
        else
        {
            /* select() will be called by us */
            S->timedwait(&app_map_listener, &my_time);
        }
    } 
    else 
    {   /* 
         * app_map_pre_select() has already been called by User
         * select() has already been called by User 
         */
        if (p_readfds==APP_NULL || p_writefds==APP_NULL )
        {
            APPL_PORT_TRACE(("APPL PORT [MAP] :: ERROR : NULL FDSET passed\n"));
			*p_ecode = APPL_ERR_INVALID_INPUT_PARAM; 
			return APP_FAILURE; 
        }
        /* Use the fdsets provided by user */
		app_map_listener.readfds = *p_readfds;
		app_map_listener.writefds = *p_writefds;
    }

    listener = &app_map_listener;

    while(1) {
        QMODULE         from, to;
        void            *buffer;
        signed char     priority;

        from = to = 0; priority = 0;
        if( (buffer = S->receive(listener, &from, &to, &priority)) == 0 ) {
                break;
        }

        qvIncoming( from, to, priority, buffer );

        if( !policy ) {
            break;
        }
    }

    W->walltime( &now );
    qvLogTime( &now );
    qvSchedule( &now ); 
	return APP_SUCCESS;			/* SPR 16426 */
}
#endif

/****************************************************************************
** Function: app_map_port_init_datastructures
****************************************************************************
** DESCRIPTION: This function initilises the datastructures of APPL port.
***************************************************************************/
void app_map_port_init_datastructures 
#ifdef ANSI_PROTO
	(void)
#else
	()
#endif
{
	app_U32bit_t i;

	app_map_select_opt = APPL_IPC_OPT_USE_SELECT; 
	app_map_read_write_opt = APPL_IPC_OPT_SOCK_NON_BLOCK; 
	app_map_socket_buffer_size = APPL_SOCKET_BUFFER_SIZE; 

	for (i = 0; i < APPL_MAX_DESTINATIONS; i++)
	{
		map_layer_info[i].instance_id = APP_FALSE;
		map_layer_info[i].entity_index = APP_FALSE;
		map_layer_info[i].variant = APP_FALSE;
		map_layer_info[i].is_valid = APP_FALSE;

		map_dmr_layer_info[i].instance_id = APP_FALSE;
		map_dmr_layer_info[i].entity_index = APP_FALSE;
		map_dmr_layer_info[i].variant = APP_FALSE;
		map_dmr_layer_info[i].is_valid = APP_FALSE;

		app_map_entity_info[i].conn_type = APPL_INVALID_CONN; 
		app_map_entity_info[i].port = 0; 
		app_map_entity_info[i].layer_cont = 0;  
		app_map_entity_info[i].entity_id = APP_FALSE;  
		app_map_entity_info[i].is_valid = APP_FALSE; 
		app_map_entity_info[i].fd =  -1;
#ifdef APP_MAP_MT_SAFE
		app_map_entity_info[i].fd_lock =  NULL;
#endif
		/* CSR Fix 1-9318794 Start */
		app_map_entity_info[i].time_stamp = NULL;
		/* CSR Fix 1-9318794 End */
		app_map_entity_info[i].cspl_service = 0;  
		app_map_entity_info[i].status = 0;  

		if ( app_map_read_write_opt == APPL_IPC_OPT_SOCK_NON_BLOCK)
		{
			app_map_entity_info_sockstate[i].read_offset = 0; 
			app_map_entity_info_sockstate[i].p_read_buffer = APP_NULL; 
			app_map_entity_info_sockstate[i].write_offset = 0; 
			app_map_entity_info_sockstate[i].p_write_buffer = APP_NULL; 
		}

	}

	for (i=0; i<APPL_MAX_CLIENT_CONN; i++)
	{
		app_map_remote_client_conn[i]=-1;
		if ( app_map_read_write_opt == APPL_IPC_OPT_SOCK_NON_BLOCK)
		{
			app_map_remote_client_sockstate[i].read_offset = 0; 
			app_map_remote_client_sockstate[i].p_read_buffer = APP_NULL; 
			app_map_remote_client_sockstate[i].write_offset = 0; 
			app_map_remote_client_sockstate[i].p_write_buffer = APP_NULL; 
		}
	}

	app_map_self.instance_id = 0; 
	app_map_self.tier = 0;
	app_map_self.layer_cont = 0;
	app_map_self.variant = 0;
	app_map_self.entity_id = 0;
	app_map_self.port_trace = 0; /* by default traces are off */
	app_map_self.system_trace = 0; /* by default traces are off */

	app_map_is_module_loaded = APP_FALSE;
	ylInit( &app_map_buffer_queue );
}

/****************************************************************************
** Function: app_map_validate_n_store_start_param
**
****************************************************************************
** DESCRIPTION: This function validates the command line parameters
**    provided by the user. This also update global datastructure namely
**    app_map_start_param.
** 
**
***************************************************************************/
#ifdef APP_DYN_LIBRARY
int	app_map_validate_n_store_start_param
#ifdef ANSI_PROTO
	( app_U32bit_t 	x_tier,
	app_U32bit_t 	x_inst_id,
    app_U8bit_t 	*p_x_a_em_ip,
    app_U32bit_t    x_a_em_port, 
    app_U8bit_t     *p_x_s_em_ip,
    app_U32bit_t    x_s_em_port, 
	app_U8bit_t     *p_x_m_ip,
    app_U8bit_t     *p_x_dirname)
#else
	(x_tier, x_inst_id,	p_x_a_em_ip, x_a_em_port, 
    p_x_s_em_ip, x_s_em_port, p_x_m_ip, p_x_dirname)
	app_U32bit_t 	x_tier;
	app_U32bit_t 	x_inst_id;
    app_U8bit_t 	*p_x_a_em_ip;
    app_U32bit_t    x_a_em_port; 
    app_U8bit_t     *p_x_s_em_ip;
    app_U32bit_t    x_s_em_port; 
	app_U8bit_t     *p_x_m_ip;
    app_U8bit_t     *p_x_dirname;
#endif
#else /* APP_DYN_LIBRARY */
int	app_map_validate_n_store_start_param
#ifdef ANSI_PROTO
	(int C, char **V )
#else
	(C, V )
	int C;
	char **V;
#endif
#endif
{

#ifndef APP_DYN_LIBRARY
	app_U32bit_t 		c;
	app_U8bit_t 		*p_temp;
	app_U8bit_t 		a_em [APPL_MAX_IPADDR_LEN + 100] = {0};
	app_U8bit_t 		s_em [APPL_MAX_IPADDR_LEN + 100] = {0};
#endif
	app_U8bit_t 		m_ip [APPL_MAX_IPADDR_LEN] = {0};
	app_U8bit_t 		a_em_ip [APPL_MAX_IPADDR_LEN] = {0};
	app_U8bit_t 		s_em_ip [APPL_MAX_IPADDR_LEN] = {0};
	app_U32bit_t 		a_em_port = 0; 
	app_U32bit_t 		s_em_port = 0; 
	app_U8bit_t 		t_flag,i_flag,a_flag,s_flag,m_flag, options_flag;
	app_U8bit_t 		d_flag = 0;
	app_U8bit_t 		a_em_complete,s_em_complete; 
	app_U32bit_t 		tier = 0; 
	app_U32bit_t 		inst_id = 0; 
	DIR					*p_direc = NULL;
	app_U8bit_t   		dir_flag = 0;
	
	t_flag=i_flag=a_flag=s_flag=m_flag=options_flag=APP_FALSE; 
	a_em_complete=s_em_complete=APP_FALSE;

#ifdef APP_DYN_LIBRARY
	tier = x_tier;
	t_flag = APP_TRUE;
	inst_id = x_inst_id;
	i_flag = APP_TRUE; 

	a_flag = APP_TRUE; 
	a_em_complete = APP_TRUE; 
	strcpy(a_em_ip, p_x_a_em_ip); 
	a_em_port = x_a_em_port; 

	s_flag = APP_TRUE; 
	s_em_complete = APP_TRUE; 
	strcpy(s_em_ip, p_x_s_em_ip); 
	s_em_port = x_s_em_port; 

	m_flag = APP_TRUE;
	strcpy(m_ip, p_x_m_ip);

  	p_direc = opendir(p_x_dirname);
	if (p_direc == NULL)
	{
		printf("APPL_PORT [MAP] :: ERROR: Invalid p_directory name\n");
        return (APP_FAILURE);
	}
	strcpy(app_map_gb_dirname, p_x_dirname);
	dir_flag = APP_TRUE;
	d_flag   = APP_TRUE;
#else
	/* Get the command line options */
    /* changes for 1.3.0.0.0_UI-7 start */
	/*amir changes S*/
	/*while ((c = getopt(C,V,"vbh:t:i:a:s:m:d:S:R:")) != EOF)*//*flag S & R for SPR-21354 Fix*/
	while ((c = getopt(C,V,"vYbh:t:i:a:s:m:d:S:R:")) != EOF) /*introducing new option Y for Syslog diabling*/
	/*amir changes E*/
	{
        /* changes for 1.3.0.0.0_UI-7 end */
		options_flag = APP_TRUE; 
		switch(c) {
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
				/* SPR-18532 Starts */
				strncpy(a_em, optarg,sizeof(a_em)-1);
				a_em[sizeof(a_em)-1]='\0';
				/* SPR-18532 Ends */
				p_temp = (app_U8bit_t *) strtok (a_em, ":"); 
				if (p_temp != NULL )
				{
					/* SPR-18532 Starts */
					strncpy(a_em_ip, p_temp,sizeof(a_em_ip)-1);
				   a_em_ip[sizeof(a_em_ip)-1]='\0';	
					/* SPR-18532 Ends */
				}
				else
				{
					a_em_complete = APP_FALSE; 
				}
				p_temp = (app_U8bit_t *)strtok( NULL, ":"); 
				if ( p_temp != NULL )
				{
					a_em_port = atoi(p_temp); 
				}
				else
				{
					a_em_complete = APP_FALSE; 
				}
				break;
			case 's' :   /* standby em info */
				s_flag = APP_TRUE; 
				s_em_complete = APP_TRUE ; 
				/* SPR-18532 Starts */
				strncpy(s_em, optarg,sizeof(s_em)-1);
				s_em[sizeof(s_em)-1]='\0';
				/* SPR-18532 Ends */
				p_temp = (app_U8bit_t *)strtok (s_em, ":"); 
				if (p_temp != NULL )
				{
					/* SPR-18532 Starts */
					strncpy(s_em_ip, p_temp,sizeof(s_em_ip)-1);
				   s_em_ip[sizeof(s_em_ip)-1]='\0';	
					/* SPR-18532 Ends */
				}
				else
				{	
					s_em_complete = APP_FALSE; 
				}
				p_temp = (app_U8bit_t *)strtok( NULL, ":"); 
				if ( p_temp != NULL )
				{
					s_em_port = atoi(p_temp); 
				}
				else
				{
					s_em_complete = APP_FALSE; 
				}
				break;
			case 'm' :   /* machine's IP address */
				m_flag = APP_TRUE;
				/* SPR-18532 Starts */
				strncpy(m_ip, optarg,sizeof(m_ip)-1);
				m_ip[sizeof(m_ip)-1]='\0';
				/* SPR-18532 Ends */
				break;
					/*amir changes S*/
			case 'Y' :
					g_syslog_to_be_used = APP_FALSE;
					break;
					/*amir changes E*/
			
			case 'd' :   /* directory name */
                d_flag = APP_TRUE;
                dir_flag = APP_TRUE;
				/* SPR-18532 Starts */
				strncpy(app_map_gb_dirname, optarg,sizeof(app_map_gb_dirname)-1);
				app_map_gb_dirname[sizeof(app_map_gb_dirname)-1]='\0';
				/* SPR-18532 Ends */
				if ((p_direc = opendir(app_map_gb_dirname)) == NULL)
				{
                   d_flag = APP_FALSE;
				}
                break;
            /* changes for 1.3.0.0.0_UI-7 start */
	    case 'b' : /* Exit Handler enable which will generate core */
			/*CSR:00002923 Fix Start*/
			g_enable_exit_handler =APP_TRUE;
			/*CSR:00002923 Fix Start*/
			break;
			/*SPR-21354 Fix Starts*/

			case 'S' :
			g_app_map_send_buff = atoi (optarg);
			g_app_map_soc_mgr_send_buff = atoi (optarg);
			break;

			case 'R':
			g_app_map_recv_buff = atoi (optarg);
			g_app_map_soc_mgr_recv_buff = atoi (optarg);
			break;
			
			/*SPR-21354 Fix Ends*/

            case 'v' :
				printf("\nAPP_MAP Version Number :%d.%d.%d.%d.%d\n",
                        APP_MAP_MAJOR_VER_NO, APP_MAP_MINOR_VER_NO, APP_MAP_PATCH_VER_NO, \
                        APP_MAP_CUSTOMER_REL_NO_VER_NO, APP_MAP_CUSTOMER_PATCH_NO_VER_NO );
				APP_EXIT(0);
            /* changes for 1.3.0.0.0_UI-7 end */

			case 'h' :       /* fall thru */
			default :
                /* Changes for 1.3.0.0.0_RSy-1 Start */
				/* using printf not APPL_PORT_TRACE */
				printf("\n\nUsage: app_map -t tier -i inst_id -a ip:port "
						"-s ip:port -m ip[:client port]\n\n"); 
				printf("-h\thelp\n"); 	
				printf("-v\tVersion number\n"); 	
				printf("-t\ttier level of map application platform process\n");
				printf("-i\tinstance id in the tier\n");
				printf("-a\tip address & port of active em\n");
				printf("-s\tip address & port of standby em\n");
				printf("-m\tmachines's ip address on which this process"
						" runs. Client port to connect to EM is optional\n");
                /* Changes for 1.3.0.0.0_RSy-1 End */
				printf("-d\t directory where logging of this "
							"instance is to be done\n");		

				return APP_FAILURE;
		}
	}  /* while */
    PRINT_VERSION

	/* checking for the inputs */
	if ( options_flag == APP_FALSE)
	{
        /* Changes for 1.3.0.0.0_RSy-1 Start */
		printf("\n\nUsage: app_map -t tier -i inst_id -a ip:port "
						"-s ip:port -m ip[:client port]\n\n"); 
				printf("-h\thelp\n"); 	
				printf("-t\ttier level of map application platform process\n");
				printf("-i\tinstance id in the tier\n");
				printf("-a\tip address & port of active em\n");
				printf("-s\tip address & port of standby em\n");
				printf("-m\tmachines's ip address on which this process"
						" runs. Client port to connect to EM is optional\n");
                /* Changes for 1.3.0.0.0_RSy-1 End */
				printf("-d\t directory where logging of this "
							"instance is to be done\n");		
				return APP_FAILURE;
	}
#endif
	if ( t_flag == APP_FALSE )
	{
		printf("APPL_PORT [MAP] :: ERROR: tier is not specified \n");
		return APP_FAILURE;
	}
	
	if (dir_flag == APP_TRUE)
	{
    	if ( d_flag == APP_FALSE )
    	{
       		 printf("APPL_PORT [MAP] :: ERROR: Directory name not Valid\n");
        	return APP_FAILURE;
    	}
		p_app_map_current_dir = app_map_gb_dirname;	
	} else {
		p_app_map_current_dir = (char *)getenv("PWD");
 		if(p_app_map_current_dir == NULL)
    	{
    		printf("Could not read environment variable PWD\n");
    		APPL_EXIT(APP_FAILURE,0);
    	}
		else
		{		
		/* SPR-18532 Starts */
		strncpy(app_map_gb_dirname,p_app_map_current_dir,sizeof(app_map_gb_dirname)-1);	
		app_map_gb_dirname[sizeof(app_map_gb_dirname)-1]='\0';
		/* SPR-18532 Ends */
		}
	}

	if ( m_flag == APP_FALSE )
	{
        /* Changes for 1.3.0.0.0_RSy-1 Start */
		printf("APPL_PORT [MAP] :: ERROR: machine IP[:client port] is not specified \n");
        /* Changes for 1.3.0.0.0_RSy-1 End */
		return APP_FAILURE;
	}
	
	if ( i_flag == APP_FALSE )
	{
		printf("APPL_PORT [MAP] :: ERROR: instance_id is not specified \n");
		return APP_FAILURE;
	}

	if ( a_flag == APP_FALSE )
	{
		printf("APPL_PORT [MAP] :: ERROR: active_em ip:port is not specified \n");
		return APP_FAILURE;
	}
	else
	{
		if ( a_em_complete == APP_FALSE )
		{
			printf("APPL_PORT [MAP] :: ERROR: active_em ip:port is not specified \n");
			return APP_FAILURE;
		}
	}

	if ( s_flag == APP_FALSE )
	{
		printf("APPL_PORT [MAP] :: ERROR: standby_em ip:port is not specified \n");
		return APP_FAILURE;
	}
	else
	{
		if ( s_em_complete == APP_FALSE )
		{
			printf("APPL_PORT [MAP] :: ERROR: standby_em ip:port is not specified \n");
			return APP_FAILURE;
		}
	}

#ifdef APP_DYN_LIBRARY
	if (tier != APP_TIER_LEVEL_3)
#else
	if (tier != APP_TIER_LEVEL_2)
#endif
	{
		/* invalid tier-id */
		printf("APPL_PORT [MAP] :: ERROR: invalid tier id (valid value BEP:2, TEP:3)\n"); 
		return APP_FAILURE;
	}

	app_map_start_param.tier = tier;
	app_map_self.tier = tier;


	app_map_entity_info[1].is_valid = APP_TRUE;
	app_map_entity_info[2].is_valid = APP_TRUE;
		
	app_map_start_param.instance_id = inst_id;
	/* In case self instance id is not specified 
	   by user then make app_map_self.instance_id == 0 , 
	   otherwise app_map_self.instance_id = app_map_start_param.instance_id (!= 0) */
/* SPR-17456 Starts */
	if ( (0 < app_map_start_param.instance_id) && 
			(app_map_start_param.instance_id < APPL_MAX_DESTINATIONS+1)  )
	{
/* SPR-17456 Ends */
		app_map_self.instance_id = app_map_start_param.instance_id; 
	}
	else
	{
		/* invalid number of arguments */	
		printf("APPL_PORT [MAP] :: ERROR: invalid instance id (valid values [1..%d])\n",APPL_MAX_DESTINATIONS); 
		return APP_FAILURE;
	}

	strcpy (app_map_entity_info[1].ipaddr, a_em_ip );
	app_map_entity_info[1].port = a_em_port + APP_MAP_EM_PORT_OFFSET ;
#ifdef APP_DYN_LIBRARY
#ifdef APP_MAP_MT_SAFE
    app_map_entity_info[1].fd_lock = qvNewLock();
#endif
#endif
	if ( s_flag == APP_TRUE )
	{
		strcpy (app_map_entity_info[2].ipaddr, s_em_ip );
		app_map_entity_info[2].port = s_em_port + APP_MAP_EM_PORT_OFFSET;
#ifdef APP_DYN_LIBRARY
#ifdef APP_MAP_MT_SAFE
        app_map_entity_info[2].fd_lock = qvNewLock();
#endif
#endif
	}
	if ( m_flag == APP_TRUE )
	{
        /* Changes for 1.3.0.0.0_RSy-1 Start */
        app_map_start_param.self_client_port = 0; /* Default value in case user does not supply
                                                     client port */
        sscanf(m_ip,"%[^:]:%hu",app_map_start_param.self_ip,&app_map_start_param.self_client_port);

        if(app_map_start_param.self_client_port !=0)
        {
            app_map_self.client_authenticate=1;
        }
        else
        {
            /* If self client port is not given at cmd line, no authentication required.
               This assumption will be cross checked in IMUP Response */
            app_map_self.client_authenticate=0;
        }
        /* Changes for 1.3.0.0.0_RSy-1 End */
	}
	else
	{
		struct utsname machine_name;
	  	if ( uname(&machine_name) >= 0)
		{
			strcpy (app_map_start_param.self_ip, machine_name.nodename );
		}
		else
		{
			printf("APPL_PORT [MAP] :: ERROR: could not determine self IP address\n");
			printf("APPL_PORT [MAP] :: Please pass -m option in start parameters\n");
			return APP_FAILURE;
		}
	}
		
	return APP_SUCCESS;
}

/****************************************************************************
** Function: app_map_pre_select
**
****************************************************************************
** DESCRIPTION: This function sets the select mask. It internally calls
**              qvSchedule(), hence processing any pending messages.
** RETURN:
**       APP_FAILURE - 0 for failure, p_ecode is set
**       APP_SUCCESS - 1 for Success.
**************************************************************************/
int app_map_pre_select
#ifdef ANSI_PROTO
		(  int *p_max_fd, 
           fd_set *p_readfds,
           fd_set *p_writefds,
           fd_set *p_excepfds,
           struct timeval *p_timeout, 
		   unsigned int *p_ecode) 
#else
		(p_max_fd, p_readfds, p_writefds,
         p_excepfds, p_timeout, p_ecode) 
		   int 		*p_max_fd; 
           fd_set 	*p_readfds;
           fd_set 	*p_writefds;
           fd_set 	*p_excepfds;
           struct timeval *p_timeout; 
		   unsigned int *p_ecode; 
#endif
{
    app_S32bit_t	i;
#ifdef APP_DYN_LIBRARY
    QTIME 			now, diff;
    const QTIME 	*p_wakeup;
    const QWAIT 	*p_W;
#endif

	*p_ecode = APPL_ERR_NO_ERROR; 
    if ((p_readfds == APP_NULL) || (p_writefds == APP_NULL) || (p_timeout==APP_NULL)) 
	{
        APPL_PORT_TRACE(("APPL PORT [MAP] :: ERROR : NULL FDSET passed\n"));
		*p_ecode = APPL_ERR_INVALID_INPUT_PARAM; 
		return APP_FAILURE; 
    }
    app_map_port_check_ipc();
	app_map_flush_pending_write_buffers();

	/* 0th index in entity info is self server port */
	/*  rest indices in entity info are local client connections */
	for (i=0; i< APPL_MAX_DESTINATIONS; i++) /* i shud'nt change in loop */
	{
		/*
		 * Filling fdread set for general server, em active, em standby
		 * and for non blocking fds
		 */
		if ( app_map_entity_info[i].is_valid == APP_TRUE )
		{
			if ( app_map_entity_info[i].conn_type != APPL_CLIENT_CONN_IN_PROGRESS  )
			{
				if ( app_map_entity_info[i].fd > 0 )
				{
					/* Poll on this file descriptor */
					/* reading only on fd for 
					   self server
					   active em
					   standby em */
					/* Also poll on the other client connections
					   estabhlished (in blocking mode) by
					   self */
			/*SPR-21327 Fix Starts Here*/
					APP_SET_MAX_FD(app_map_entity_info[i].fd);
			/*SPR-21327 Fix Ends Here*/
					FD_SET(app_map_entity_info[i].fd, p_readfds);
				}
				/* if the write_offset corresponding to this fd is non-zero
				   then add this socket for write_fd */
			}
			else
			{
				if ( app_map_entity_info[i].fd > 0 )
				{
					/* Poll on this file descriptor */
			/*SPR-21327 Fix Starts Here*/
					APP_SET_MAX_FD(app_map_entity_info[i].fd);
			/*SPR-21327 Fix Ends Here*/
					FD_SET(app_map_entity_info[i].fd, p_writefds);
				}
			}
		}

		/* 
		 * adding remote client connections in read fd 
		 * remote client means client connection accepted by this process
		 */
		if ( app_map_remote_client_conn[i] > 0 )
		{
			/*SPR-21327 Fix Starts Here*/
			APP_SET_MAX_FD(app_map_remote_client_conn[i]);
			/*SPR-21327 Fix Ends Here*/
			FD_SET(app_map_remote_client_conn[i], p_readfds);
		}

	}
	/* computing p_timeout value */
#ifdef APP_DYN_LIBRARY
    p_W = &app_map_shell_waitstruct;
    p_W->walltime( &now );
    qvLogTime( &now );
    p_wakeup = qvSchedule( &now );
    if( p_wakeup ) 
	{
        if( qvTimeDiff(p_wakeup, &now, &diff) < 0 ) 
		{
            diff.s = diff.us = 0;
        }
    } 
	else 
	{
		/* default timed wait */
        diff.s = 3;
        diff.us = 100;
    }
    p_timeout->tv_sec = diff.s;
    p_timeout->tv_usec = diff.us;
#endif
			/*SPR-21327 Fix Starts Here*/
	/* *p_max_fd = (FD_SETSIZE); */
	*p_max_fd = max_fd_val;
			/*SPR-21327 Fix Ends Here*/
	return 	APP_SUCCESS; 
}

#ifdef APP_DYN_LIBRARY
/****************************************************************************
** Function: app_map_send_to_app_map
**
****************************************************************************
** DESCRIPTION: Service User (e.g. MAP-U) uses this function for sending
**              any message octet buffer API to APPL. 
**              The buffer sent to this function is freed by APPL, therefore,
**              memory for p_buf must be allocated using ss7_mem_get function.
**
** RETURN:
**       0 on failure, p_ecode contains error code
**       1 on success.
**
***************************************************************************/
int app_map_send_to_app_map
#ifdef ANSI_PROTO
	(unsigned char *p_msg, unsigned int *p_ecode) 
#else
	(p_msg, p_ecode) 
	unsigned char 	*p_msg;
	unsigned int 	*p_ecode; 
#endif
{
	app_U8bit_t		ret_val = APP_SUCCESS;

    APPL_PORT_TRACE(("APPL PORT [MAP] :: MAP-DMR: Process Incoming APIs\n"));

	ret_val = app_map_dmr_process_user_mesg(p_msg, p_ecode);

	if (ret_val == APP_FAILURE)
	{
    	APPL_PORT_TRACE(("APPL PORT [MAP] :: MAP-DMR: Process Incoming APIs"
					" Failed : Ecode [%d]\n", *p_ecode));
	}

	return (ret_val);
}

#else

/****************************************************************************
** Function: ss7p_send_to_ss7p_user
****************************************************************************
**
***************************************************************************/

int ss7p_send_to_ss7p_user (unsigned char *p_buffer, unsigned short noctets)
{
	unsigned int error = 0;

	/* Now the beauty of avoding memcopy */
	/* the buffer has 5 bytes of platfrom whihc has to cram enough info
		for correct data transfer */
	/* we'll make first bytes (src id) as 0x00 to help the map mhandler */
	/* api_id takes place of the version id place( as version id is 2 ) */

/* SPR-16419 Starts */
	if(p_buffer[0] == SS7P_SAP_READY_IND)
	{
		/* SAP-Ready API exposed : sap status indication */
		app_U8bit_t		*p_out_buffer = APP_NULL;
		
		p_out_buffer = (app_U8bit_t *)app_mem_get(APP_HEADER_SIZE + sizeof(app_sm_sap_status_t));

		((app_sm_sap_status_t *)(p_out_buffer + APP_HEADER_SIZE))->sap = p_buffer[5];
		if(p_buffer[6] == SAP_STATUS_NOT_READY)
		{
			((app_sm_sap_status_t *)(p_out_buffer + APP_HEADER_SIZE))->state = APP_SAP_DOWN;
		}
		else
		if(p_buffer[6] == SAP_STATUS_READY)
		{
			((app_sm_sap_status_t *)(p_out_buffer + APP_HEADER_SIZE))->state = APP_SAP_UP;
			app_tcap_instance_status = APP_INSTANCE_ALIVE;

			/* Following code put here to delay the MAP instance marked alive 
				until TCAP SAP indication is reciebed, else USER instance starts
				pumping data before SAP UP by TCAP */
			if (app_map_instance_status == (APP_INSTANCE_ALIVE + 1))
			{	
				app_map_instance_status = APP_INSTANCE_ALIVE;
				app_compute_ent_stat_sent_frm_ent_response(); /* SPR Fix 20519 */

			}	
			
			if ((app_map_instance_status == APP_INSTANCE_ALIVE) &&
				(app_map_bulk_i_am_active_sent == APP_FALSE))
			{
				app_map_bulk_i_am_active_sent = APP_TRUE;
				if (APP_FAILURE == app_map_send_bulk_i_am_active_to_ss7p(&error))
				{
					APPL_PORT_TRACE(("APP MAP [UMR] :: Function app_map_send_bulk_i_am_active_to_ss7p returned Failure with error-code=[%d]\n", error));
				}
			}			
		}
		else
		{
			APPL_PORT_TRACE(("Invalid SAP State[%d] received in SAP-Ready Indication\n", (app_U8bit_t)(p_buffer[6])));
			ss7p_mem_free(p_buffer);
			/* SPR-18532 Starts */
			app_mem_free(p_out_buffer);
			/* SPR-18532 Ends */
			return APP_SUCCESS;
		}

		ss7p_mem_free(p_buffer);
		APP_SET_SRC_ID(p_out_buffer, MAP_EM_MODULE_ID);
		APP_SET_API_ID(p_out_buffer, APP_SAP_STATUS_CHANGE);
		APP_SET_LENGTH(p_out_buffer,(APP_HEADER_SIZE + sizeof(app_sm_sap_status_t)));

		if (app_map_is_module_loaded == APP_TRUE)
		{
			qvSend(qvGetService(APP_MAP_UMR_MODULE_ID),10,(void *)p_out_buffer);
		}
		else
		{
			/* add this message to app_buffer_queue */
            APPL_PORT_TRACE(("APPL PORT [MAP] :: INFO: Buffering in app_buffer_queue\n")); 
            APP_MAP_PUSH_BQ(p_out_buffer);
		}
	}
	else
	if (p_buffer[0] != 0xff)
	{
		/* not extended to send it as soon as possible */
		p_buffer[1] = p_buffer[0];
		p_buffer[0] = 0x00;
		/* Patch fix for SPR-17456 */
		if (app_map_is_module_loaded == APP_TRUE)
		{
           qvSend(qvGetService(APP_MAP_MODULE_ID),10,(void *)p_buffer);
        }
	}
	else
	{
		APPL_PORT_TRACE(("Invalid SS7P Extended API received at SS7P User interface\n"));
		ss7p_mem_free(p_buffer);
	}
/* SPR-16419 Ends */

	return (APP_SUCCESS);
}


int app_map_send_i_am_active_to_ss7p(unsigned int instance_id, int *p_error)
{
	unsigned char *p_tcap_buffer = APP_NULL;

	/* Will send message to TCAP UMR that i'm MAP */
	p_tcap_buffer = ss7p_mem_get(15);
	p_tcap_buffer[0] = 255;
	p_tcap_buffer[1] = 0;
	p_tcap_buffer[2] = 0;
	p_tcap_buffer[3] = 15;
	p_tcap_buffer[4] = 0;
	p_tcap_buffer[5] = 255;
	p_tcap_buffer[6] = 255;
	p_tcap_buffer[7] = 0;
	p_tcap_buffer[8] = SS7P_USER_PART;
	p_tcap_buffer[9] = SS7P_TCAP_USER_PART;
	p_tcap_buffer[10] = 0;
	p_tcap_buffer[11] = instance_id;
	p_tcap_buffer[12] = app_map_self.instance_id;
	p_tcap_buffer[13] = 1; /*APP_INST_MAP*/
	p_tcap_buffer[14] = 1; /*variant*/
		
	if(APP_FAILURE == ss7p_send_to_ss7p(p_tcap_buffer, p_error))
	{
		APPL_PORT_TRACE(("Unable to send I'm MAP message to FEP-Instance[%d] due to error=[%d]", instance_id, *p_error));
		return (APP_FAILURE);
	}
	return (APP_SUCCESS);
}

int app_map_send_bulk_i_am_active_to_ss7p(int *p_error)
{
	unsigned int i = 0;
	
	for (i = 0; i < app_map_num_active_fep_count; i++)
	{
		if (APP_FAILURE == app_map_send_i_am_active_to_ss7p(app_map_num_active_fep_list[i], p_error))
		{
			APPL_PORT_TRACE(("Function app_map_send_i_am_active_to_ss7p returned Failure with error-code=[%d]",*p_error));
		}	
	}

	return APP_SUCCESS;
}
#endif

/* SPR 20843 Starts */
/****************************************************************************
** Function: 
****************************************************************************
** app_map_user_port_umr_initialize()
***************************************************************************/
app_void_t app_map_user_port_umr_initialize(app_void_t)
{
	  char app_map_user_port_soname[APP_MAP_USER_PORT_SO_NAME_LEN];

		bzero(app_map_user_port_soname,sizeof(app_map_user_port_soname));
		app_map_user_port_umr_global.dllhandle = APP_NULL;
		app_map_user_port_umr_global.dll_app_map_user_port_umr_init = APP_NULL;
		app_map_user_port_umr_global.dll_app_map_user_port_get_tep_id = APP_NULL;	

		SPL_TRACE(("APP MAP [MAIN] :: Function app_map_user_port_umr_initialize is called.\n"));
		
		if (app_map_get_user_port_umr_so_name(app_map_user_port_soname))
		{
				 if((app_map_user_port_umr_global.dllhandle = dlopen((const char *)app_map_user_port_soname, \
																															RTLD_LAZY))	== APP_NULL)
				 {
						SPL_TRACE(("APP MAP [MAIN] :: Unable to open the APP MAP USER PORT UMR dll from value present in APP_MAP_USER_PORT_SO_NAME environment variable. Or APP_MAP_USER_PORT_SO_NAME environment	variable is not set. Trying Default Path.\n"));
#ifdef APPL_OS_SOLARIS
						strcpy(app_map_user_port_soname,"../lib/SunOS/libapp_map_user_port_umr.so");
#endif
#ifdef APPL_OS_LINUX
						strcpy(app_map_user_port_soname,"../lib/Linux/libapp_map_user_port_umr.so");
#endif

				 if((app_map_user_port_umr_global.dllhandle = dlopen((const char *)\
																	app_map_user_port_soname, RTLD_LAZY)) == APP_NULL)
				 {
							SPL_TRACE(("APP MAP [MAIN] :: Unable to open the APP MAP USER PORT UMR dll. Please check environment variable APP_MAP_USER_PORT_SO_NAME or ensure that the library is libapp_map_user_port_umr.so is created.\n"));
				 }
				 else
				 {
							SPL_TRACE(("APP MAP [MAIN] :: libapp_map_user_port_umr.so SUCCESSFULLY LOADED FROM PATH: [%s]\n",app_map_user_port_soname));
							app_map_user_port_umr_global.dll_app_map_user_port_umr_init = (app_void_t (*)(app_void_t)) \
																		dlsym(app_map_user_port_umr_global.dllhandle,"app_map_user_port_umr_init");
					
							app_map_user_port_umr_global.dll_app_map_user_port_get_tep_id = (app_U8bit_t (*) (app_U8bit_t,app_U8bit_t,app_U8bit_t,app_U8bit_t*)) \
																		dlsym(app_map_user_port_umr_global.dllhandle,"app_map_user_port_get_tep_id");	

							if(app_map_user_port_umr_global.dll_app_map_user_port_umr_init == APP_NULL)
							{
														SPL_TRACE(("APP MAP [MAIN] :: Unable to map dll_app_map_user_port_umr_init from libapp_map_user_port_umr dll\n"));
							}
							if(app_map_user_port_umr_global.dll_app_map_user_port_get_tep_id == APP_NULL)
							{
														SPL_TRACE(("APP MAP [MAIN] :: Unable to map dll_app_map_user_port_get_tep_id from libapp_map_user_port_umr dll\n"));
							}
				 }
			 }
			 else
			 {
					SPL_TRACE(("APP MAP [MAIN] :: libapp_map_user_port_umr.so SUCCESSFULLY LOADED FROM PATH: [%s]\n",app_map_user_port_soname));
					app_map_user_port_umr_global.dll_app_map_user_port_umr_init = (app_void_t (*)(app_void_t)) \
																		dlsym(app_map_user_port_umr_global.dllhandle,"app_map_user_port_umr_init");
					app_map_user_port_umr_global.dll_app_map_user_port_get_tep_id = (app_U8bit_t (*) (app_U8bit_t,app_U8bit_t,app_U8bit_t,app_U8bit_t*)) \
																		dlsym(app_map_user_port_umr_global.dllhandle,"app_map_user_port_get_tep_id");
					if(app_map_user_port_umr_global.dll_app_map_user_port_umr_init == APP_NULL)
					{
											SPL_TRACE(("APP MAP [MAIN] :: Unable to map dll_app_map_user_port_umr_init from libapp_map_user_port_umr dll\n"));
					}
					if(app_map_user_port_umr_global.dll_app_map_user_port_get_tep_id == APP_NULL)
					{
											SPL_TRACE(("APP MAP [MAIN] :: Unable to map dll_app_map_user_port_get_tep_id from libapp_map_user_port_umr dll\n"));
					}
			 }
		}
		else
		{
				SPL_TRACE(("APP MAP [MAIN] :: Unable to open the APP MAP USER PORT UMR dll. OS Detection Failed\n"));
		}		
}
/****************************************************************************
 ** Function: app_map_get_user_port_umr_so_name
 **
 ****************************************************************************
 ** DESCRIPTION: This function returns failure if it is not able to find the 
 ** shared library name for a particular OS
 ** 
 ** 
 **
 ***************************************************************************/
app_U8bit_t app_map_get_user_port_umr_so_name (char *name)
{
	char               *path;
	path = getenv(APP_MAP_USER_PORT_SO_NAME);
	if (path == (char *) 0)
	{
		SPL_TRACE(("APP MAP [MAIN] :: APP_MAP_USER_PORT_SO_NAME environment variable not set\n"));
		SPL_TRACE(("APP MAP [MAIN] :: Attempting to set default shared library path\n"));
#ifdef APPL_OS_SOLARIS
		strcpy(name,"../lib/SunOS/libapp_map_user_port_umr.so");
#endif
#ifdef APPL_OS_LINUX
		strcpy(name,"../lib/Linux/libapp_map_user_port_umr.so");
#endif

	}
	else
	{
		if(strlen(path) > APP_MAP_USER_PORT_SO_NAME_LEN)
		{
			SPL_TRACE(("APP MAP [MAIN] :: APP_MAP_USER_PORT_SO_NAME shared library path is too big.\n"));
			return APP_FAILURE;
		}
		else if(strlen(path) == 0)
		{
			SPL_TRACE(("APP MAP [MAIN] :: APP_MAP_USER_PORT_SO_NAME ENV VAR length is 0.Using Default Path.\n"));
			
#ifdef APPL_OS_SOLARIS
		strcpy(name,"../lib/SunOS/libapp_map_user_port_umr.so");
#endif
#ifdef APPL_OS_LINUX
		strcpy(name,"../lib/Linux/libapp_map_user_port_umr.so");
#endif
			
		}
		else
		{
			strcpy(name, path);
		}

	}
	SPL_TRACE(("APP MAP [MAIN] :: Shared library path for libapp_map_user_port_umr.so is [%s]\n",name));
	return APP_SUCCESS;

}
/* SPR 20843 Ends */

/*SPR-21354 Fix Starts*/
#ifdef APP_DYN_LIBRARY
void app_map_user_buffer_request(unsigned int user_send_sock_buff, unsigned int user_recv_sock_buff)
{
		if(0 != user_send_sock_buff)
				g_app_map_send_buff = user_send_sock_buff;
		if(0 != user_recv_sock_buff)
				g_app_map_recv_buff = user_recv_sock_buff;
}
#endif
/*SPR-21354 Fix Ends*/

/* SPR#21474 FIX STARTS */
/****************************************************************************
** Function: app_map_get_user_lib_ver
**
****************************************************************************
** DESCRIPTION: Service User (e.g. MAP-U) uses this function for getting the 
                version of app_map user library.
**
** RETURN:
**       0 on failure, if p_user_lib_ver is NULL pointer
**       1 on success.
**
***************************************************************************/
int app_map_get_user_lib_ver(app_get_user_lib_ver_t *p_user_lib_ver)
{
    app_U8bit_t ret_val=APP_FAILURE;
    if((char *)0 != p_user_lib_ver)
    {
        p_user_lib_ver->major_ver_no = APP_MAP_MAJOR_VER_NO;
        p_user_lib_ver->minor_ver_no = APP_MAP_MINOR_VER_NO;
        p_user_lib_ver->patch_ver_no = APP_MAP_PATCH_VER_NO;
        p_user_lib_ver->customer_rel_ver_no = APP_MAP_CUSTOMER_REL_NO_VER_NO;
        p_user_lib_ver->customer_patch_ver_no = APP_MAP_CUSTOMER_PATCH_NO_VER_NO;
        ret_val=APP_SUCCESS; 
    }
    return ret_val;
}
/* SPR#21474 FIX ENDS */
/* SPR#21539 start */
#ifdef APP_DYN_LIBRARY
int app_map_get_tep_api_stat(int api_id, int *no_of_beps, app_get_tep_api_stats_t *p_tep_stats, int reset)
{
	app_U8bit_t ret_val=APP_FAILURE;
	int i,count=0;
	int actual_bep_cnt = 0;
	if(NULL == no_of_beps)
	{
		APPL_PORT_TRACE(("MAP DMR::app_map_get_tep_api_stat:BEP Instance count is NULL\n"));
		return APPL_ERR_INVALID_INPUT_PARAM;
	}
	actual_bep_cnt = (int)app_map_get_bep_inst_count();
	if(0 == actual_bep_cnt)
	{
		APPL_PORT_TRACE(("MAP DMR::app_map_get_tep_api_stat:No active BEP available.\n"));
		return APP_SUCCESS;
	}
	if (actual_bep_cnt > *no_of_beps)
	{
		APPL_PORT_TRACE(("MAP DMR::app_map_get_tep_api_stat:BEP Instance count[%d] is lesser than actual count[%d].\n",\
			*no_of_beps,actual_bep_cnt));
		return APPL_ERR_ISUFFICIENT_INST_CNT;
	}
	else
	{
		if(actual_bep_cnt != *no_of_beps)
		APPL_PORT_TRACE(("MAP DMR::app_map_get_tep_api_stat:BEP Instance count[%d] is greater than actual count[%d].\n",\
			*no_of_beps,actual_bep_cnt));
		*no_of_beps = actual_bep_cnt;
	}
	if(NULL != p_tep_stats)
	{
		if (!(api_id%4))
		{
			api_id -=  3;
		}
		else
		{
			api_id = (api_id - ( api_id % 4 ) + 1);
		}
		if (((api_id <= MAP_COMMON_API_MAX)) || ((api_id >= MAP_SRV_USR_API_BASE) && (api_id <= MAP_SUPPORTED_FEAT_CONFIRM)))
		{
			for(i=0;i<APPL_MAX_DESTINATIONS;i++)
			{
				if (map_layer_info[i].is_valid == APP_TRUE)
				{
					if (count >= *no_of_beps)
					{
						break;
					}
					p_tep_stats[count].request = app_map_api_lib_stat_table[api_id][i];
					p_tep_stats[count].indication = app_map_api_lib_stat_table[api_id + 1 ][i];
					p_tep_stats[count].response = app_map_api_lib_stat_table[api_id + 2 ][i];
					p_tep_stats[count].confirm = app_map_api_lib_stat_table[api_id + 3 ][i];
					p_tep_stats[count].bep_inst_id = map_layer_info[i].instance_id;

					if(1 == reset)
					{
#ifdef APP_MAP_MT_SAFE
						qvLock(app_map_user_stat_lock);
#endif
						app_map_api_lib_stat_table[api_id][i] = 0;
						app_map_api_lib_stat_table[api_id + 1 ][i] = 0;
						app_map_api_lib_stat_table[api_id + 2 ][i] = 0;
						app_map_api_lib_stat_table[api_id + 3 ][i] = 0;
#ifdef APP_MAP_MT_SAFE
						qvUnlock(app_map_user_stat_lock);
#endif
					}
					count +=1;

				}
			}
		}
		else
		{
			APPL_PORT_TRACE(("MAP DMR::app_map_get_tep_api_stat:API-ID is Invalid\n"));
			return APPL_ERR_INVALID_MAP_API_ID;
		}
		ret_val=APP_SUCCESS; 
	}
	else
	{
		APPL_PORT_TRACE(("MAP DMR::app_map_get_tep_api_stat:app_get_tep_api_stats_t is NULL.\n"));
		return APPL_ERR_INVALID_INPUT_PARAM;
	}
	return ret_val;
}
#endif
/* SPR#21539 end */

