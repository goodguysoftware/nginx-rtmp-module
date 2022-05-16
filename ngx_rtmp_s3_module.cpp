
/*
 * Copyright (C) Tran Duong
 */


extern "C" {
    #include <ngx_config.h>
    #include <ngx_core.h>
    #include "ngx_rtmp.h"
    #include "ngx_rtmp_cmd_module.h"
    #include "ngx_rtmp_s3_module.h"
}

#include <iostream>
#include <fstream>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/PutObjectRequest.h>

using namespace Aws;


SDKOptions                              options;


static ngx_rtmp_publish_pt              next_publish;


static ngx_int_t ngx_rtmp_s3_postconfiguration(ngx_conf_t *cf);
static void * ngx_rtmp_s3_create_app_conf(ngx_conf_t *cf);
static char * ngx_rtmp_s3_merge_app_conf(ngx_conf_t *cf,
       void *parent, void *child);
static ngx_int_t ngx_rtmp_s3_init_process(ngx_cycle_t *cycle);
static void ngx_rtmp_s3_exit_process(ngx_cycle_t *cycle);


typedef struct {
    S3Crt::S3CrtClient                 *client;
} ngx_rtmp_s3_ctx_t;


typedef struct {
    ngx_flag_t                          s3;
    ngx_str_t                           endpoint;
    ngx_str_t                           region;
    ngx_str_t                           key_id;
    ngx_str_t                           secret_key;
    ngx_str_t                           bucket;
    ngx_str_t                           prefix;
} ngx_rtmp_s3_app_conf_t;


static ngx_command_t ngx_rtmp_s3_commands[] = {

    { ngx_string("s3"),
      NGX_RTMP_MAIN_CONF|NGX_RTMP_SRV_CONF|NGX_RTMP_APP_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_flag_slot,
      NGX_RTMP_APP_CONF_OFFSET,
      offsetof(ngx_rtmp_s3_app_conf_t, s3),
      NULL },

    { ngx_string("s3_endpoint"),
      NGX_RTMP_MAIN_CONF|NGX_RTMP_SRV_CONF|NGX_RTMP_APP_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_RTMP_APP_CONF_OFFSET,
      offsetof(ngx_rtmp_s3_app_conf_t, endpoint),
      NULL },

    { ngx_string("s3_region"),
      NGX_RTMP_MAIN_CONF|NGX_RTMP_SRV_CONF|NGX_RTMP_APP_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_RTMP_APP_CONF_OFFSET,
      offsetof(ngx_rtmp_s3_app_conf_t, region),
      NULL },

    { ngx_string("s3_key_id"),
      NGX_RTMP_MAIN_CONF|NGX_RTMP_SRV_CONF|NGX_RTMP_APP_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_RTMP_APP_CONF_OFFSET,
      offsetof(ngx_rtmp_s3_app_conf_t, key_id),
      NULL },

    { ngx_string("s3_secret_key"),
      NGX_RTMP_MAIN_CONF|NGX_RTMP_SRV_CONF|NGX_RTMP_APP_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_RTMP_APP_CONF_OFFSET,
      offsetof(ngx_rtmp_s3_app_conf_t, secret_key),
      NULL },

    { ngx_string("s3_bucket"),
      NGX_RTMP_MAIN_CONF|NGX_RTMP_SRV_CONF|NGX_RTMP_APP_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_RTMP_APP_CONF_OFFSET,
      offsetof(ngx_rtmp_s3_app_conf_t, bucket),
      NULL },

    { ngx_string("s3_prefix"),
      NGX_RTMP_MAIN_CONF|NGX_RTMP_SRV_CONF|NGX_RTMP_APP_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_RTMP_APP_CONF_OFFSET,
      offsetof(ngx_rtmp_s3_app_conf_t, prefix),
      NULL },

    ngx_null_command
};


static ngx_rtmp_module_t  ngx_rtmp_s3_module_ctx = {
    NULL,                               /* preconfiguration */
    ngx_rtmp_s3_postconfiguration,      /* postconfiguration */

    NULL,                               /* create main configuration */
    NULL,                               /* init main configuration */

    NULL,                               /* create server configuration */
    NULL,                               /* merge server configuration */

    ngx_rtmp_s3_create_app_conf,        /* create location configuration */
    ngx_rtmp_s3_merge_app_conf,         /* merge location configuration */
};


ngx_module_t  ngx_rtmp_s3_module = {
    NGX_MODULE_V1,
    &ngx_rtmp_s3_module_ctx,            /* module context */
    ngx_rtmp_s3_commands,               /* module directives */
    NGX_RTMP_MODULE,                    /* module type */
    NULL,                               /* init master */
    NULL,                               /* init module */
    ngx_rtmp_s3_init_process,           /* init process */
    NULL,                               /* init thread */
    NULL,                               /* exit thread */
    ngx_rtmp_s3_exit_process,           /* exit process */
    NULL,                               /* exit master */
    NGX_MODULE_V1_PADDING
};


ngx_int_t
ngx_rtmp_s3_upload(ngx_rtmp_session_t *s, ngx_str_t path, ngx_str_t key, ngx_str_t content_type, char cache)
{
    ngx_rtmp_s3_app_conf_t              *sacf;
    ngx_rtmp_s3_ctx_t                   *ctx;
    u_char                              *obj_key;
    u_char                              *p;
    std::shared_ptr<IOStream>            bodyStream;
    S3Crt::Model::PutObjectRequest       request;
    S3Crt::Model::PutObjectOutcome       outcome;

    sacf = (ngx_rtmp_s3_app_conf_t *)ngx_rtmp_get_module_app_conf(s, ngx_rtmp_s3_module);
    if (sacf == NULL || !sacf->s3) {
        return NGX_OK;
    }

    obj_key = (u_char *)ngx_pcalloc(s->connection->pool, (size_t)sacf->prefix.len + 1 + key.len);
    p = ngx_cpymem(obj_key, sacf->prefix.data, sacf->prefix.len);
    if (sacf->prefix.len > 0) {
        *(p++) = '/';
    }
    p = ngx_cpymem(p, key.data,key.len);

    ngx_log_error(NGX_LOG_INFO, s->connection->log, 0,
                  "s3: uploading '%V' to '%s'", &path, obj_key);

    ctx = (ngx_rtmp_s3_ctx_t *)ngx_rtmp_get_module_ctx(s, ngx_rtmp_s3_module);

    request.SetBucket((char *)sacf->bucket.data);
    request.SetKey((char *)obj_key);
    request.SetACL(S3Crt::Model::ObjectCannedACL::public_read);

    if (content_type.len > 0) {
        request.SetContentType((char *)content_type.data);
    }

    if (!cache) {
        request.SetCacheControl("no-cache");
    }

    bodyStream = MakeShared<Aws::FStream>("ngx_rtmp_s3", (char *)path.data, std::ios_base::in | std::ios_base::binary);
    if (!bodyStream->good()) {
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                      "s3: failed to open file '%s'", path.data);
        return NGX_ERROR;
    }
    request.SetBody(bodyStream);

    outcome = ctx->client->PutObject(request);

    if (outcome.IsSuccess()) {
        ngx_log_error(NGX_LOG_INFO, ngx_cycle->log, 0,
                      "s3: file '%s' uploaded", obj_key);
    } else {
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                      "s3: failed to upload file '%s'", obj_key);
    }

    return NGX_OK;
}


static ngx_int_t
ngx_rtmp_s3_publish(ngx_rtmp_session_t *s, ngx_rtmp_publish_t *v)
{
    ngx_rtmp_s3_app_conf_t        *sacf;
    ngx_rtmp_s3_ctx_t             *ctx;
    S3Crt::ClientConfiguration     config;
    S3Crt::S3CrtClient            *cli;

    sacf = (ngx_rtmp_s3_app_conf_t *)ngx_rtmp_get_module_app_conf(s, ngx_rtmp_s3_module);
    if (sacf == NULL || !sacf->s3) {
        goto next;
    }

    ctx = (ngx_rtmp_s3_ctx_t *)ngx_rtmp_get_module_ctx(s, ngx_rtmp_s3_module);

    if (ctx == NULL) {

        ctx = (ngx_rtmp_s3_ctx_t *)ngx_pcalloc(s->connection->pool, sizeof(ngx_rtmp_s3_ctx_t));
        ngx_rtmp_set_ctx(s, ctx, ngx_rtmp_s3_module);

    } else {

        cli = ctx->client;

        ngx_memzero(ctx, sizeof(ngx_rtmp_s3_ctx_t));

        ctx->client = cli;
    }

    if (ctx->client == NULL) {
        config.throughputTargetGbps = 5;
        config.partSize = 8 * 1024 * 1024; // 8 MB.

        if (sacf->endpoint.len > 0) {
            config.endpointOverride = (char *)sacf->endpoint.data;
        }

        if (sacf->region.len > 0) {
            config.region = (char *)sacf->region.data;
        }

        ctx->client = new S3Crt::S3CrtClient(Auth::AWSCredentials((char *)sacf->key_id.data, (char *)sacf->secret_key.data),
                                             config, Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                             sacf->endpoint.len == 0,
                                             S3Crt::US_EAST_1_REGIONAL_ENDPOINT_OPTION::NOT_SET);
        
        auto outcome = ctx->client->ListBuckets();

        if (outcome.IsSuccess()) {
            ngx_log_error(NGX_LOG_INFO, s->connection->log, 0, "s3: found %i buckets",
                          outcome.GetResult().GetBuckets().size());
        } else {
            ngx_log_error(NGX_LOG_ERR, s->connection->log, 0, "s3: error: %s",
                          outcome.GetError().GetMessage().c_str());
            
            return NGX_ERROR;
        }
    }

    ngx_snprintf(v->args, NGX_RTMP_MAX_ARGS, "%s%ss3=on",
                 v->args, ngx_strlen(v->args) > 0 ? "&" : "");

next:
    return next_publish(s, v);
}


static ngx_int_t
ngx_rtmp_s3_postconfiguration(ngx_conf_t *cf)
{
    next_publish = ngx_rtmp_publish;
    ngx_rtmp_publish = ngx_rtmp_s3_publish;

    return NGX_OK;
}


static void *
ngx_rtmp_s3_create_app_conf(ngx_conf_t *cf)
{
    ngx_rtmp_s3_app_conf_t *conf;

    conf = (ngx_rtmp_s3_app_conf_t *)ngx_pcalloc(cf->pool, sizeof(ngx_rtmp_s3_app_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    conf->s3 = NGX_CONF_UNSET;

    return conf;
}


static char *
ngx_rtmp_s3_merge_app_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_rtmp_s3_app_conf_t    *prev = (ngx_rtmp_s3_app_conf_t *)parent;
    ngx_rtmp_s3_app_conf_t    *conf = (ngx_rtmp_s3_app_conf_t *)child;

    ngx_conf_merge_value(conf->s3, prev->s3, 0);
    ngx_conf_merge_str_value(conf->endpoint, prev->endpoint, "");
    ngx_conf_merge_str_value(conf->region, prev->region, "");
    ngx_conf_merge_str_value(conf->key_id, prev->key_id, "");
    ngx_conf_merge_str_value(conf->secret_key, prev->secret_key, "");
    ngx_conf_merge_str_value(conf->bucket, prev->bucket, "");
    ngx_conf_merge_str_value(conf->prefix, prev->prefix, "");
    
    return NGX_CONF_OK;
}


static ngx_int_t
ngx_rtmp_s3_init_process(ngx_cycle_t *cycle)
{
    InitAPI(options);
    return NGX_OK;
}


static void
ngx_rtmp_s3_exit_process(ngx_cycle_t *cycle)
{
    ShutdownAPI(options);
}