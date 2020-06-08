package com.king.Elasticsearch.sink;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.apache.flink.util.ExceptionUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;


import java.io.IOException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @Author: king
 * @Date: 2019-08-30
 * @Desc: TODO
 */

public class ElasticSearchSinkUtil {
    private static Logger log= Logger.getLogger(ElasticSearchSinkUtil.class);
    /**
     *
     * @param hosts es hosts
     * @param bulkFlunshMaxActions
     * @param parallelism   并行数
     * @param data  数据
     * @param func
     * @param <T>
     */
    public static <T> void addSink(List<HttpHost> hosts, int bulkFlunshMaxActions, int parallelism,
                                   SingleOutputStreamOperator<T> data, ElasticsearchSinkFunction<T> func){
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(hosts,func);
        esSinkBuilder.setBulkFlushMaxActions(bulkFlunshMaxActions);
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        data.addSink(esSinkBuilder.build()).setParallelism(parallelism);
    }

    public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
        String[] hostsList =hosts.split(",");
        List<HttpHost> addresses =new ArrayList<>();
        for (String host:hostsList){
            if(host.startsWith("http")){
                URL url =new URL(host);
                addresses.add(new HttpHost(url.getHost(),url.getPort()));
            }else {
                String[] parts =host.split(":",2);
                if(parts.length>1){
                    addresses.add(new HttpHost(parts[0],Integer.parseInt(parts[1])));
                }else {
                    throw new MalformedURLException("invclid elasticsearch host format");
                }
            }
        }
        return addresses;
    }
    public static class RetryRejetedExecutionFailureHandler implements ActionRequestFailureHandler{
        public RetryRejetedExecutionFailureHandler(){}

        @Override
        public void onFailure(ActionRequest actionRequest, Throwable throwable, int i, RequestIndexer requestIndexer) throws Throwable {
            if(ExceptionUtils.findThrowable(throwable, EsRejectedExecutionException.class).isPresent()){
                requestIndexer.add(new ActionRequest[] {actionRequest});
            }else {
                if(ExceptionUtils.findThrowable(throwable, SocketTimeoutException.class).isPresent()){
                    // 忽略写入超时，因为ElasticSearchSink 内部会重试请求，不需要抛出来去重启 flink job
                    return;
                }else {
                    Optional<IOException> exp =ExceptionUtils.findThrowable(throwable,IOException.class);
                    if(exp.isPresent()){
                        IOException ioExp = exp.get();
                        if(ioExp!=null && ioExp.getMessage()!=null && ioExp.getMessage().contains("max retry timeout")){
                            log.error(ioExp.getMessage());
                            return;
                        }
                    }
                }
                throw throwable;
            }
        }
    }
}
