package com.example.im.controller;

import cn.hutool.core.util.RandomUtil;
import com.example.im.common.util.ApiResult;
import com.example.im.common.util.Constants.AppVule;
import com.example.im.common.util.MyStringUtil;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/rocket")
public class RocketMqController {

    private static final Logger log = LoggerFactory.getLogger(RocketMqController.class);


    @Autowired
    private AppVule appVule;



    //发送数据
    @RequestMapping(value = "/send", method = RequestMethod.POST)
    @ResponseBody
    public ApiResult send(@RequestParam Map map) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer =new DefaultMQProducer(RandomUtil.randomUUID());
        producer.setNamesrvAddr("192.168.204.130:9876");
        producer.start();
        for (int i=0;i<1;i++){
            Message msg=new Message("TopicTest_a","TagA","这是内容".getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult=producer.send(msg);
            log.info("发送消息:"+sendResult.toString());
        }


        return ApiResult.success(1);
    }


    //接受数据,如果不返回 CONSUME_SUCCESS,否则会一直接受到消息,
    //虽然发送的是130服务器,但是消费者nameAddres写成131,也可以接受到消息
    @RequestMapping(value = "/receive", method = RequestMethod.POST)
    @ResponseBody
    public ApiResult receive(@RequestParam Map map) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {



        DefaultMQPushConsumer consumer=new DefaultMQPushConsumer(RandomUtil.randomUUID());
        consumer.setNamesrvAddr("192.168.204.131:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("TopicTest_a","*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                log.info("consumer收到消息:"+list);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();


        DefaultMQPushConsumer consumer1=new DefaultMQPushConsumer(RandomUtil.randomUUID());
        consumer1.setNamesrvAddr("192.168.204.130:9876");
        consumer1.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer1.subscribe("TopicTest_a","*");
        consumer1.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                log.info("consumer_1收到消息:"+list);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer1.start();


        return ApiResult.success(1);
    }



//    @RequestMapping(value = "token",method = RequestMethod.POST)
//
//    @ResponseBody
//    public ApiResult token(@RequestParam Map map){
//
//        log.info(Convert.toStr(map.get("userId")));
//
//        long startTime=new Date().getTime();
//        log.info("开始执行时间:"+  startTime);
//
//            TokenBean tokenBean = (TokenBean) redisTemplateForSessionDb.opsForValue().get("user:"+Convert.toStr(map.get("id")));
//
//           // log.info("token:"+tokenBean.getToken());
//
//
//        long timex=DateUtil.spendMs(startTime);
//
//        log.info("执行时间差:"+  timex);
//
//
//        return ApiResult.success("获取token成功");
//
//
//    }
//
//
//
//
//    @RequestMapping(value = "setken",method = RequestMethod.POST)
//
//    @ResponseBody
//    public ApiResult setken(@RequestParam Map map){
//        long startTime=new Date().getTime();
//      log.info("开始执行时间:"+  startTime);
//
//            String token=appVule.robotNames;
//            TokenBean tb=new TokenBean();
//            tb.setToken(token);
//           redisTemplateForSessionDb.opsForValue().set("user:"+Convert.toStr(map.get("id")),tb);
//           // log.info("设置token成功");
//
//
//
//
//        long timex=DateUtil.spendMs(startTime);
//
//        log.info("执行时间差:"+  timex);
//
//
//        return ApiResult.success("设置token成功");
//    }
//










}
