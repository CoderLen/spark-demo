package com.liny.crawler;
import com.liny.crawler.bean.ZhihuData;

import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.selector.Selectable;

public class GithubRepoPageProcessor implements PageProcessor {

    private Site site = Site.me().setRetryTimes(3).setSleepTime(1000);

    @Override
    public void process(Page page) {
        page.addTargetRequests(page.getHtml().links().regex("(http://www.zhihu.com/question/[0-9]+)").all());
        page.putField("author", page.getUrl().regex("http://www.zhihu.com/question/[0-9]+*").toString());
        ZhihuData zhihuData = new ZhihuData();
		zhihuData.setUrl(page.getUrl().get());
        String title = page.getHtml().xpath("h2[class=zm-item-title zm-editable-content]").toString();
        page.putField("title", title);
        zhihuData.setTitle(title);
		System.out.println("title:" + title);

		Selectable tagStr = page.getHtml().xpath("a[class=zm-item-tag]");
		page.putField("tags", tagStr);
		zhihuData.setTags(tagStr.toString());
		String zhaiyao = page.getHtml().xpath("div[class=zm-editable-content]").toString();
		zhihuData.setZhaiyao(zhaiyao);
		Selectable answers = page.getHtml().xpath("div[class=zm-item-answer  zm-item-expanded]");
		page.putField("answers", answers);
    }

    @Override
    public Site getSite() {
        return site;
    }

    public static void main(String[] args) {
        Spider.create(new GithubRepoPageProcessor()).addUrl("https://www.zhihu.com").thread(5).run();
    }
}