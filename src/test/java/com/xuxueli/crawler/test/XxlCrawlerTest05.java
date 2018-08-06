package com.xuxueli.crawler.test;

import com.xuxueli.crawler.XxlCrawler;
import com.xuxueli.crawler.annotation.PageFieldSelect;
import com.xuxueli.crawler.annotation.PageSelect;
import com.xuxueli.crawler.conf.XxlCrawlerConf;
import com.xuxueli.crawler.model.PageLoadInfo;
import com.xuxueli.crawler.parser.PageParser;
import com.xuxueli.crawler.util.JsoupUtil;
import com.xuxueli.crawler.util.ProxyIpUtil;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.*;

/**
 * 爬虫示例05：爬取公开的免费代理，生成动态代理池
 * (免费代理可从ip181或kxdaili获取，免费代理不稳定可以多试几个；仅供学习测试使用，如有侵犯请联系删除； )
 *
 * @author xuxueli 2017-10-09 19:48:48
 */
public class XxlCrawlerTest05 {
    private static Logger logger = LoggerFactory.getLogger(XxlCrawlerTest05.class);

    @PageSelect(cssQuery = "#ip_list tbody > tr:gt(0)")
    public static class PageVo {

        @PageFieldSelect(cssQuery = "td:eq(1)")
        private String ip;

        @PageFieldSelect(cssQuery = "td:eq(2)")
        private int port;


        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        @Override
        public String toString() {
            return "PageVo{" +
                    "ip='" + ip + '\'' +
                    ", port=" + port +
                    '}';
        }
    }

    public static void main(String[] args) {

        // 代理池
        final List<PageVo> proxyPool = new ArrayList<PageVo>();

        // 构造爬虫
        XxlCrawler crawler = new XxlCrawler.Builder()
                .setFailRetryCount(3)
                .setPauseMillis(1000)
                .setUrls("http://www.xicidaili.com/nn/1")
                .setWhiteUrlRegexs("http://www\\.xicidaili\\.com/nn/[1]")
                .setThreadCount(1)
                .setPageParser(new PageParser<PageVo>() {
                    @Override
                    public void parse(Document html, Element pageVoElement, PageVo pageVo) {
                        if (pageVo.getPort() == 0) {
                            return;
                        }

                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(pageVo.getIp(), pageVo.getPort()));
                        if (ProxyIpUtil.checkProxy(proxy, "http://whatismyip.akamai.com/") == 200) {
                            proxyPool.add(pageVo);
                            logger.info("proxy pool size : {}, new proxy: {}", proxyPool.size(), pageVo);
                        }


                    }
                })
                .build();

        // 启动
        crawler.start(true);

        // 代理池数据
        logger.info("----------- proxy pool total size : {} -----------", proxyPool.size());
        logger.info(proxyPool.toString());

        // 校验代理池
        logger.info("----------- proxy pool check -----------");
        if (proxyPool.size() > 0) {
            for (PageVo pageVo : proxyPool) {
                try {
                    Document html = JsoupUtil.load(new PageLoadInfo("http://whatismyip.akamai.com/",
                            null,
                            null,
                            null,
                            XxlCrawlerConf.USER_AGENT_CHROME,
                            null,
                            false,
                            XxlCrawlerConf.TIMEOUT_MILLIS_DEFAULT,
                            new Proxy(Proxy.Type.HTTP, new InetSocketAddress(pageVo.getIp(), pageVo.getPort()))));
                    logger.info(pageVo + " : " + html.html());
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }

    }

}
