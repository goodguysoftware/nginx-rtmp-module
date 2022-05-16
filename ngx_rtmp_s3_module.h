
/*
 * Copyright (C) Tran Duong
 */


#ifndef _NGX_RTMP_S3_H_INCLUDED_
#define _NGX_RTMP_S3_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_rtmp.h"


extern ngx_module_t                     ngx_rtmp_record_module;


ngx_int_t ngx_rtmp_s3_upload(ngx_rtmp_session_t *s, ngx_str_t path, ngx_str_t key,
                             ngx_str_t content_type, char cache);


#endif /* _NGX_RTMP_S3_H_INCLUDED_ */
