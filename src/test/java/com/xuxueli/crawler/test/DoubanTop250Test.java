package com.xuxueli.crawler.test;

import com.xuxueli.crawler.XxlCrawler;
import com.xuxueli.crawler.annotation.PageFieldSelect;
import com.xuxueli.crawler.annotation.PageSelect;
import com.xuxueli.crawler.parser.PageParser;
import com.xuxueli.crawler.rundata.strategy.LocalRunData;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lomoye on 2018/8/6.
 */
public class DoubanTop250Test {

    @PageSelect(cssQuery = "#content")
    public static class PageVo {

        @PageFieldSelect(cssQuery = ".top250 .top250-no")
        private String no;

        @PageFieldSelect(cssQuery = "h1 span")
        private String title;

        @PageFieldSelect(cssQuery = "#interest_sectl .rating_num")
        private String score;

        private String link;

        public String getNo() {
            return no;
        }

        public void setNo(String no) {
            this.no = no;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getScore() {
            return score;
        }

        public void setScore(String score) {
            this.score = score;
        }

        public String getLink() {
            return link;
        }

        public void setLink(String link) {
            this.link = link;
        }

        @Override
        public String toString() {
            return "PageVo{" +
                    "no='" + no + '\'' +
                    ", title='" + title + '\'' +
                    ", score='" + score + '\'' +
                    ", link='" + link + '\'' +
                    '}';
        }
    }


    public static void main(String[] args) {

        final AtomicInteger counter = new AtomicInteger(0);
        final List<DoubanTop250Test.PageVo> pageVos = new ArrayList<>();

        final Lock lock = new ReentrantLock();

        LocalRunData runData = new LocalRunData();

        XxlCrawler crawler = new XxlCrawler.Builder()
                .setRunData(runData)
                .setFailRetryCount(10)
                .setPauseMillis(2000)
                .setUrls("https://movie.douban.com/top250")
                .setWhiteUrlRegexs("https://movie\\.douban\\.com/subject/\\d+/", "https://movie\\.douban\\.com/top250\\?start=\\d+&filter=")
                .setTargetUrlRegex("https://movie\\.douban\\.com/subject/\\d+/")
                .setAllowTargetUrlSpread(false)
                .setThreadCount(1)
                .setPageParser(new PageParser<DoubanTop250Test.PageVo>() {
                    @Override
                    public void parse(Document html, Element pageVoElement, DoubanTop250Test.PageVo pageVo) {
                        // 解析封装 PageVo 对象
                        String pageUrl = html.baseUri();
                        System.out.println("标记：" + pageUrl + "：" + pageVo.toString());
                        counter.getAndIncrement();

                        lock.lock();
                        pageVo.setLink(pageUrl);
                        pageVos.add(pageVo);

                        lock.unlock();
                    }
                })
                .build();

        System.out.println("start");
        crawler.start(true);
        System.out.println("end");
        System.out.println("总共解析数:" + counter);
        System.out.println("总添加url数:" + runData.getTotalAddCounter());
        System.out.println("总实际添加url数:" + runData.getActualAddCounter());
        System.out.println("总共重复url数:" + runData.getRepeatCounter());
        System.out.println("总共取出url数:" + runData.getTakeCounter());

        pageVos.forEach(System.out::println);
    }


}
