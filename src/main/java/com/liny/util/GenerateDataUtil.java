package com.liny.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 产生模拟数据工具类
 */
public class GenerateDataUtil {
    private static Logger LOG = LoggerFactory.getLogger(GenerateDataUtil.class);

    private static List<String> PROVINCES = new ArrayList<String>();
    private static List<String> CITYS = new ArrayList<String>();
    private static List<String> STREET_NUM = new ArrayList<String>();
    private static Random rand = new Random();

    // 初始化静态数据
    static {
        for (int i = 1; i < 1000; i++) {
            STREET_NUM.add(i + "号");
        }
        initCitiesData("cities.txt");
    }

    public static String FIRST_NAME =
            "赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨朱秦尤许何吕施张孔曹严华金魏陶姜戚谢邹喻柏水窦章云苏潘葛奚范彭郎鲁韦昌马苗凤花方俞任袁柳酆鲍史唐费廉岑薛雷贺倪汤滕殷罗毕郝邬安常乐于时傅皮卞齐康伍余元卜顾孟平黄和穆萧尹姚邵湛汪祁毛禹狄米贝明臧计伏成戴谈宋茅庞熊纪舒屈项祝董梁杜阮蓝闵席季麻强贾路娄危江童颜郭梅盛林刁钟徐邱骆高夏蔡田樊胡凌霍虞万支柯咎管卢莫经房裘缪干解应宗宣丁贲邓郁单杭洪包诸左石崔吉钮龚程嵇邢滑裴陆荣翁荀羊於惠甄魏加封芮羿储靳汲邴糜松井段富巫乌焦巴弓牧隗山谷车侯宓蓬全郗班仰秋仲伊宫宁仇栾暴甘钭厉戎祖武符刘姜詹束龙叶幸司韶郜黎蓟薄印宿白怀蒲台从鄂索咸籍赖卓蔺屠蒙池乔阴郁胥能苍双闻莘党翟谭贡劳逄姬申扶堵冉宰郦雍却璩桑桂濮牛寿通边扈燕冀郏浦尚农温别庄晏柴瞿阎充慕连茹习宦艾鱼容向古易慎戈廖庚终暨居衡步都耿满弘匡国文寇广禄阙东殴殳沃利蔚越夔隆师巩厍聂晁勾敖融冷訾辛阚那简饶空曾毋沙乜养鞠须丰巢关蒯相查后江红游竺权逯盖益桓公万俟司马上官欧阳夏侯诸葛闻人东方赫连皇甫尉迟公羊澹台公冶宗政濮阳淳于仲孙太叔申屠公孙乐正轩辕令狐钟离闾丘长孙慕容鲜于宇文司徒司空亓官司寇仉督子车颛孙端木巫马公西漆雕乐正壤驷公良拓拔夹谷宰父谷粱晋楚阎法汝鄢涂钦段干百里东郭南门呼延归海羊舌微生岳帅缑亢况后有琴梁丘左丘东门西门商牟佘佴伯赏南宫墨哈谯笪年爱阳佟第五言福百家姓续";
    public static String LAST_NAME =
            "秀娟英华慧巧美娜静淑惠珠翠雅芝玉萍红娥玲芬芳燕彩春菊兰凤洁梅琳素云莲真环雪荣爱妹霞香月莺媛艳瑞凡佳嘉琼勤珍贞莉桂娣叶璧璐娅琦晶妍茜秋珊莎锦黛青倩婷姣婉娴瑾颖露瑶怡婵雁蓓纨仪荷丹蓉眉君琴蕊薇菁梦岚苑婕馨瑗琰韵融园艺咏卿聪澜纯毓悦昭冰爽琬茗羽希宁欣飘育滢馥筠柔竹霭凝晓欢霄枫芸菲寒伊亚宜可姬舒影荔枝思丽伟刚勇毅俊峰强军平保东文辉力明永健世广志义兴良海山仁波宁贵福生龙元全国胜学祥才发武新利清飞彬富顺信子杰涛昌成康星光天达安岩中茂进林有坚和彪博诚先敬震振壮会思群豪心邦承乐绍功松善厚庆磊民友裕河哲江超浩亮政谦亨奇固之轮翰朗伯宏言若鸣朋斌梁栋维启克伦翔旭鹏泽晨辰士以建家致树炎德行时泰盛雄琛钧冠策腾楠榕风航弘";
    private static String[] road =
            "五山路,广西街,港青街,台东八广场,济南街,吉林支路,西陵峡二街,安庆路,利津路,虹桥广场,寿张街,闽江一大厦,禹城街,台东六路,黄埔大道,茌平路,南九水街,威海广场,高雄广场,南街一路,宁国三大厦,东阿街,西横四路,观海二广场,屏东支路,黄台支广场,黄岛路,南海支路,大港一广场,台南路,西康街,金乡广场,兰山路,贵州街,澳门五路,驼峰路,湛山四大厦,王村路,新湛一路,荷泽四路,汶水街,无棣街,肥城街,诸城大厦,高苑大厦,包头路,龙岩街,泰州三广场,寿光广场,龙口街,福寺大厦,高唐街,乐清广场,菜市二路,埕口路,宜兴大厦,博平街,芝泉路,三明路,山西广场,漳浦广场,三明南路,太湖路,珠海街,章丘路,澳门九广场,台西纬一广场,延安一街,澄海路,北环四路,吴兴三广场,流亭立交桥,团岛四街,大连支街,王家麦岛,标山路,团岛二路,仙居街,江城大厦,海阳路,大港纬三街,聊城大厦,长城大厦,高邮湖街,四方街,福州北路,临清路,齐河路,丽水街,孟庄广场,丹东街,台西四街,吴兴大厦,沾化街,东四路,青大街,华阳路,台西纬五路,台东西七街,河城大厦,瑞阳街,四平路,明阳大厦,澄海二街,邱县路,营口路,宁武关广场,花莲街,金门路,宁国路,峄县路,郓城北广场,新湛三街,观象二路,丰县广场,丰海路,苏州街,新安路,台湾街,齐东路,燕儿岛路,蒲台大厦,南平广场,居庸关街,云霄大厦,小港二街,重庆大厦,黑龙江路,汶上大厦,湛山二街,盐城街,定陶街,胶东广场,台东一街,吴县二大厦,扬州街,莒县路,泰兴广场,兴安大厦,龙口西路,台西三路,大成大厦,古田路,临淄路,台北路,历城广场,棣纬二大厦,徐州大厦,新昌街,台东四路,刘家峡路,秋阳路,松江路,松山广场,广东广场,大港纬一路,漳州街,三明北街,大尧三路,莲岛大厦,东三路,泰山支路,芝罘路,天津路,如东路,平原广场,海门路,通化路,泰州六街,长兴街,正阳路,汕头街,贮水山大厦,顺兴街,夏庄街道,浙江路,大尧二路,上海支路,辽宁路,海口街,新湛路,闽江大厦,龙口西,宁阳广场,香港西大厦,澳门四街,红山峡支路,澳门二路,滋阳街,台东东七广场,丹阳街,东海西路,泗水大厦,濮县街,祁山路,秀湛四路,郭口大厦,郯城街,琴屿大厦,姜沟路,二轻新村镇,澳门三路,台东二路,基隆广场,李村支广场,江西支街,湖北街,泰州五街,镇江街,秀湛路,陵县大厦,昌邑街,明月峡大厦,单县路,泉州街,香港中大厦,德阳路,云门二路,铁山广场,新湛支路,金口二街,钜野广场,无棣二大厦,珠海二街,德县路,无棣纬一广场,沈阳支大厦,大港三街,华严路,金乡东路,瑞云街,青铜峡路,荣成路,太平角五路,澳门六广场,礼阳路,山城广场,临邑路,天河南路,龙羊峡路,红山峡路,黄县路,城阳街,河北大厦,永嘉大厦,高密路,太清路广州路,龙口东,招远街,福建广场,台湛广场,沂水路,台西纬二路,福清广场,台西五路,阳谷广场,阳信路,嘉祥路,台西纬四街,西横一路,范县路,淄川广场,掖县路,风岗街,西藏路,振兴街,保定街,泰州一路,鱼台街,长安南路,曲阜街,南街四路,锦州支街,张店大厦,江宁广场,即墨路,惠民南路,和兴路,中山街,湛山一街,北京街,绍兴广场,文阳街,港华大厦,青岛路,阳明广场,磁山路,善化街,江苏广场,无棣一路,泰安路,遵义路,小港一大厦,汉口路,东五路,小港沿,白沙河路,北环二路,伏龙山路,太平路,大尧一广场,漳平路,新田路,武城广场,莱芜一广场,黄海街,太平角四路,洪泽湖路,大港纬二路,庆祥街,宁波路,龙口东路,逍遥一街,大港纬四街,石牌东路,潍县广场,莱阳街,澄海一路,朝阳街,台西一大厦,夏津大厦,云溪广场,日照街,太平角一街,龙泉路,杭州支广场,北环三路,天台东一路,咸阳支街,桓台路,西环,恒山路,瞿塘峡街,滕县路,伏龙街,吴兴一广场,大学广场,武胜关广场,沧口街,瑞金广场,巨野大厦,嘉峪关路,中山大道,鄱阳湖大厦,晓望支街,平定路,海江街,华山路,吴县一街,港通路,巫峡大厦,西陵峡街,康城街,南口广场,屏东广场,商河路,人和路,平度广场,朝城路,河南广场,合江路,旅顺街,黄台广场,内蒙古路,兴安支街,丰盛街,港夏街,惠民街,龙华街,登州路,团岛路,澳门一路,济阳路,南通大厦,胶州广场,澳门七广场,沛县路,青城广场,吉林路,新竹路,体育西路,西陵峡三路,吴淞路,荷泽三大厦,石岛广场,邹县广场,硕阳街,莘县路,东平街,无棣三街,太平山,奉化街,仙山西大厦,宁国一支广场,鱼山广场,桑梓路,台西纬三广场,冠县支广场,堂邑路,金田路,微山湖街,仙游路,东海东大厦,安邱街,太清路,普集支路,宁国一大厦,旌德路,东五街,东光大厦,珠海一广场,安徽路,邹平路,文登广场,福山支广场,南京广场,恩县广场,珠海支街,大港沿,石牌西路,泰州街,冠县路,长清街,天台一广场,台东西二路,春阳路,湛山路,咸阳广场,益都街,龙山路,博兴路,观音峡广场,昌乐街,民航街,平阴路,四川路,泰城街,上杭路,澳门八街,贮水山支街,临淮关大厦,崇阳街,春城街,沈阳街,和阳广场,体育东路,长城南路,长春街,江南大厦,市场纬街,西横二路,莱州路,辽北街,泰州二路,博山大厦,六码头,大连街,东海中街,天台二路,西横三路,国城路,石村广场,海川路,东二路,莆田街,大港纬五路,太平角二广场,青海街,昌平街,山东路,天台东二街,武定路,大港二路,周村大厦,北环一路,观海一路,武昌路,市场三街,大港四街,华城路,广西支街,澄海三路,安城街,江西街,天河北,逍遥三大厦,团岛一街,闽江二广场,馆陶街,澳门广场,西陵峡一大厦,仰口街,云南路,漳州路一路,莱芜二路,东海一大厦,滨县广场,宝山广场,福山大厦,清平路,南街三路,金口一广场,芙蓉山村,青大一街,延安二广场,泰州四路,费县路,延安三路,枣庄广场,韶关路,鹊山广场,赵红广场,常州街,银川西路,彰化大厦,机场路,临朐路,锦城广场,紫荆关街,宁国二支路,港环路,菜市一街,港联路,万寿路,山海关路,台西二街,南街二路,济宁支街,新湛二路,红岛路,寿康路,观城路,金口三路,金湖大厦,仙山东路,流亭大厦,泰山广场,郓城南路,惜福镇街道,城武大厦,新泰大厦,承德街,漳州街二街,普集路,嫩江广场,曹县路,明水路,闽江三路,福州南路,巢湖街,恒台街,湘潭街,渤海街,南阳街,西山路,无棣四路,天河东路,荷泽二街,龙江街,新疆路,青威高速,牟平街,广饶支街,白塔广场,济宁街,岳阳路,长山路,宁夏路,李村街,芙蓉路,市场一路,甘肃广场,台东七大厦,郧阳路,北环,新浦路,四平支路,湛山三路,西江广场,长安路,正阳关街,大麦岛,南村广场,京山广场,东山路,延吉街,闽江四街,宁国四街,隆德广场,宁国二路,浦口广场,顺城路,洞庭湖广场,汇泉广场,宁德街,晓望街,德盛街,雒口大厦,祚山路,仙山街,昆明路,中川路,荷泽一路,大名路,天台路,荷泽路,海泊路,青海支路,湛山五街,鱼山支街,清和街,嘉义街,热河大厦,秀湛二路,信号山路,团岛三街,双元大厦,海游路,德平广场,太平角六街,十梅庵街,艳阳街,台东三街,上海路,兴阳路,易州广??,高田广场,台东东二路,南海大厦,道口路,太平角三大厦,三门峡路,城武支大厦,陵县支街,蒙阴大厦,吴兴二大厦,绣城路,锦州街,市场二大厦,湖南路,长汀街,南街,乐陵街,信号山支路,洮南大厦,天河东,红岛支大厦,古庙工业园,北仲三路,郭口东街,山口路,西藏二街,徐家麦岛函谷关广场,宁海路,大沽街,云门一街,中城路,西藏一广场,广州路"
                .split(",");
    public static String base = "abcdefghijklmnopqrstuvwxyz0123456789";
    private static final String[] email_suffix =
            "@gmail.com,@yahoo.com,@msn.com,@hotmail.com,@aol.com,@ask.com,@live.com,@qq.com,@0355.net,@163.com,@163.net,@263.net,@3721.net,@yeah.net,@googlemail.com,@126.com,@sina.com,@sohu.com,@yahoo.com.cn"
                .split(",");
    private List<String> data;
    private int threadNum;
    private int seqNo = 0;


    public GenerateDataUtil(List<String> sampleData, int threadNum) {
        data = sampleData;
        this.threadNum = threadNum;
    }


    public GenerateDataUtil() {
    }


    /**
     * 初始化城市数据
     * 
     * @param citesFile
     */
    public static void initCitiesData(String citesFile) {
        LOG.info("初始化城市数据{}开始", citesFile);
        try {
            BufferedReader in =
                    new BufferedReader(new InputStreamReader(
                        GenerateDataUtil.class.getResourceAsStream(citesFile), "utf-8"));
            String line = "";
            int num = 0;
            while ((line = in.readLine()) != null) {
                String province = line.split("#")[0].trim();
                PROVINCES.add(province);
                for (String city : line.split("#")[1].split("、")) {
                    num++;
                    CITYS.add(province + city.trim());
                }
            }
            in.close();
        }
        catch (Exception e) {
            LOG.info("加{}数据失败", citesFile);
        }
        LOG.info("初始化城市数据{}结束,省份{}个,城市{}个", citesFile, PROVINCES.size(), CITYS.size());
    }


    /**
     * 随机生成名字
     * 
     * @return
     */
    public static String generateName() {
        int nameIndex = rand.nextInt(FIRST_NAME.length());
        StringBuffer name = new StringBuffer();
        name.append(FIRST_NAME.charAt(nameIndex));
        int nameIndex2 = rand.nextInt(LAST_NAME.length());
        name.append(LAST_NAME.charAt(nameIndex2));
        if (nameIndex2 % 2 == 0) {
            name.append(LAST_NAME.charAt(rand.nextInt(LAST_NAME.length())));
        }
        return name.toString();
    }


    /**
     * 随机城市
     */
    public static String getRandomCity() {
        int num = rand.nextInt(CITYS.size());
        return CITYS.get(num);
    }


    /**
     * 随机省份
     * 
     * @return
     */
    public static String getRandomProvince() {
        int num = rand.nextInt(PROVINCES.size());
        return PROVINCES.get(num);
    }


    /**
     * 随机生成城市地址
     * 
     * @param city
     * @return
     */
    public static String getRandomAddr(String city) {
        StringBuffer addr = new StringBuffer(city);
        int num = rand.nextInt(road.length);
        addr.append(road[num]);
        num = rand.nextInt(STREET_NUM.size());
        addr.append(STREET_NUM.get(num));
        return addr.toString();
    }

    /**
     * 返回手机号码
     */
    private static String[] telFirst =
            "134,135,136,137,138,139,150,151,152,157,158,159,130,131,132,155,156,133,153".split(",");


    public static String getTel() {
        int index = getNum(0, telFirst.length - 1);
        String first = telFirst[index];
        String second = String.valueOf(getNum(1, 888) + 10000).substring(1);
        String third = String.valueOf(getNum(1, 9100) + 10000).substring(1);
        return first + second + third;
    }


    /**
     * 返回Email
     * 
     * @param lMin
     *            最小长度
     * @param lMax
     *            最大长度
     * @return
     */
    public static String getEmail(int lMin, int lMax) {
        int length = getNum(lMin, lMax);
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = (int) (Math.random() * base.length());
            sb.append(base.charAt(number));
        }
        sb.append(email_suffix[(int) (Math.random() * email_suffix.length)]);
        return sb.toString();
    }


    public static int getNum(int start, int end) {
        return (int) (Math.random() * (end - start + 1) + start);
    }


    /**
     * 获取特定长度的随机数字字符串
     */
    public static String randomNumberStr(int length){
      String numstr = "0123456789";
      StringBuffer newStr = new StringBuffer();
      for (int i=0;i<length;i++) {
        int randomIndex = rand.nextInt(numstr.length());
        newStr.append(numstr.charAt(randomIndex));
      }
      return newStr.toString();
    }


    public static void main(String[] args) {
        System.out.println(GenerateDataUtil.randomNumberStr(5));
//        for (int i = 0; i < 100; i++) {
//            String city = GenerateDataUtil.getRandomCity();
//            System.out.println(GenerateDataUtil.getRandomAddr(city) + "  " + GenerateDataUtil.generateName());
//            // System.out.println(task.getEmail(5, 9));
//        }
    }
}
