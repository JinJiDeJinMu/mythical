package com.hs.utils;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * TODO: 仅默认空间默认项目表迁移
 * 批量建表:
 *    ODS-----ADS
 *    use by 【测试】--映射-->【生产】
 */
public class BatchCreateTable {

    public static String hdfsUrl = "hdfs://183.66.128.84:8020";
    public static List<Map> tableList = new ArrayList<>();
    static Gson gson = MyGson.create(true);
    static String cookie = "XXL_JOB_LOGIN_IDENTITY=7b226964223a312c22757365726e616d65223a2261646d696e222c2270617373776f7264223a226531306164633339343962613539616262653536653035376632306638383365222c22726f6c65223a312c227065726d697373696f6e223a6e756c6c7d; language=zh_CN; AMBARISESSIONID=node0owzog68eip7dapw6nstilg0m2024.node0; sessionId=2e386adc-6635-4ce2-b61b-9141da292c0d; huoshiDataToken=bearereyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX25hbWUiOiJhZG1pbiIsInN5c3RlbVVzZXJJZCI6IjE1MTAxNDkyNzI1OTgzMTkxMDUiLCJzY29wZSI6WyJyZWFkIiwid3JpdGUiXSwiYWNjb3VudFR5cGUiOiJTWVNURU0iLCJsb2dpblRva2VuIjoiMTY5Mjc1Njk3OTk0NSIsInN5c3RlbVVzZXJuYW1lIjoiYWRtaW4iLCJhdmF0YXIiOiJodHRwOi8vMTAuMC4wLjk6OTAwMS9lY2l0eW9zL2RlZmF1bHQvaW1hZ2VzL2RlZmF1bHRfaWNvbi5QTkciLCJleHAiOjE2OTI3NTg3ODAsInVzZXJJZCI6IjE1MTAxNDkyNzI1OTgzMTkxMDUiLCJqdGkiOiJlNmNjMWM0MC05ZWFhLTQzM2UtODMzNy1jODAzOWJhZDc1NWQiLCJjbGllbnRfaWQiOiJKOEtxMTY5aDU2dG0iLCJ1c2VybmFtZSI6ImFkbWluIn0.T5d2nXLIebIxZ5ECxpGqSu-GtcnT6bKXymd4JSQ_qfA; username=admin";
    static String Authorization = "bearereyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX25hbWUiOiJhZG1pbiIsInN5c3RlbVVzZXJJZCI6IjE1MTAxNDkyNzI1OTgzMTkxMDUiLCJzY29wZSI6WyJyZWFkIiwid3JpdGUiXSwiYWNjb3VudFR5cGUiOiJTWVNURU0iLCJsb2dpblRva2VuIjoiMTY5Mjc1Njk3OTk0NSIsInN5c3RlbVVzZXJuYW1lIjoiYWRtaW4iLCJhdmF0YXIiOiJodHRwOi8vMTAuMC4wLjk6OTAwMS9lY2l0eW9zL2RlZmF1bHQvaW1hZ2VzL2RlZmF1bHRfaWNvbi5QTkciLCJleHAiOjE2OTI3NTg3ODAsInVzZXJJZCI6IjE1MTAxNDkyNzI1OTgzMTkxMDUiLCJqdGkiOiJlNmNjMWM0MC05ZWFhLTQzM2UtODMzNy1jODAzOWJhZDc1NWQiLCJjbGllbnRfaWQiOiJKOEtxMTY5aDU2dG0iLCJ1c2VybmFtZSI6ImFkbWluIn0.T5d2nXLIebIxZ5ECxpGqSu-GtcnT6bKXymd4JSQ_qfA";
    static String baseUrl = "http://183.66.128.84:28082";

    public static void main(String[] args) {

        // test
        /*
        tableList = queryTableList("stg");
        System.out.println("==1" + tableList);

        tableList = queryTableListForAll("stg");
        System.out.println("==2" + tableList);

        for (Map row : tableList) {
            System.out.println("id: " + row.get("id"));
            System.out.println("code:" + row.get("code"));

            System.out.println("tableId: " + getTableId((String) row.get("name")));
            System.out.println(getTableFieldByDelta((String) row.get("code")));
        }
        */
        /*
        // 1.STG层表创建  大写
        Map<String, String> stgDataSetIdMap = new HashMap<>();//LinkedHashMap<>();
        stgDataSetIdMap.put("STG","11579350456685379585");


        stgDataSetIdMap.put("stg_1676037883875549186","1676037891760852994"); // 测试项目
        stgDataSetIdMap.put("stg_1676039580198895618","1676039583608733698"); // 测试项目
        stgDataSetIdMap.put("stg_1676167630311915521","1676167635286364162"); // 测试项目
        stgDataSetIdMap.put("stg_1677241516185030658","1677241521415258113"); // 测试项目
        stgDataSetIdMap.put("stg_1678338873123344386","1678338876709539842"); // 测试项目


        for (String layer : stgDataSetIdMap.keySet()){
            String datasetId = stgDataSetIdMap.get(layer);
            createTableByDataTable(layer,datasetId);
        }
        */

        // 2.创建 ods-----ads   小写
        ArrayList<String> layerList = new ArrayList<>();
        layerList.add("stg");
        layerList.add("ods");
        layerList.add("dwd");
        layerList.add("dws");
        layerList.add("ads");
/*
        layerList.add("ods_1676037883875549186"); // 测试项目
        layerList.add("ods_1676039580198895618"); // 测试项目
        layerList.add("ods_1676167630311915521"); // 测试项目
        layerList.add("ods_1677241516185030658"); // 测试项目
        layerList.add("ods_1678338873123344386"); // 测试项目

        layerList.add("dwd");
        layerList.add("dwd_1676037883875549186"); // 测试项目
        layerList.add("dwd_1676039580198895618"); // 测试项目
        layerList.add("dwd_1676167630311915521"); // 测试项目
        layerList.add("dwd_1677241516185030658"); // 测试项目
        layerList.add("dwd_1678338873123344386"); // 测试项目

        layerList.add("dws");
        layerList.add("dws_1676037883875549186"); // 测试项目
        layerList.add("dws_1676039580198895618"); // 测试项目
        layerList.add("dws_1676167630311915521"); // 测试项目
        layerList.add("dws_1677241516185030658"); // 测试项目
        layerList.add("dws_1678338873123344386"); // 测试项目

        layerList.add("ads");
        layerList.add("ads_1676037883875549186"); // 测试项目
        layerList.add("ads_1676039580198895618"); // 测试项目
        layerList.add("ads_1676167630311915521"); // 测试项目
        layerList.add("ads_1677241516185030658"); // 测试项目
        layerList.add("ads_1678338873123344386"); // 测试项目
        */
        for (String layer : layerList) {
            // 下线
            //publishAllModel(layer,false);
            // 发布
            //publishAllModel(layer,true);

            // sop
            publishAllModelV2(layer);
        }

    }


    /**
     * 发布所有模型
     */
    public static void publishAllModel(String layer, boolean isPublish) {
        try {
            tableList = queryTableListForAll(layer);

            Configuration configuration = new Configuration();
            configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            org.apache.hadoop.fs.FileSystem fileSystem = org.apache.hadoop.fs.FileSystem.get(new URI(hdfsUrl), configuration, "hdfs");
            System.out.println("表数量:" + tableList.size());
            for (Map m : tableList) {
                try {
                    publishOneModel((String) m.get("id"), layer.toLowerCase(), isPublish);
                    //等待建表 未建成功继续等待
                    Thread.sleep(1200L);
                    while (!isExist(fileSystem, layer.toLowerCase(), (String) m.get("code"))) Thread.sleep(1000L);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception ee) {
            ee.printStackTrace();
        }
    }

    /**
     * 发布所有模型建表后
     * 恢复原有任务状态
     */
    public static void publishAllModelV2(String layer) {
        try {
            tableList = queryTableListForAll(layer);

            Configuration configuration = new Configuration();
            configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            org.apache.hadoop.fs.FileSystem fileSystem = org.apache.hadoop.fs.FileSystem.get(new URI(hdfsUrl), configuration, "hdfs");
            System.out.println("表数量:" + tableList.size());

            for (Map m : tableList) {
//                if (m.get("code").equals("stg_deda_building_info_econ_wzw_test")) {
                // 任务为下线状态
                if (m.get("status").equals("OFFLINE")) {
                    try {
                        publishOneModelV2((String) m.get("id"), layer.toLowerCase(), true);
                        //等待建表 未建成功继续等待
                        Thread.sleep(1200L);
//                            while (!isExist(fileSystem, layer.toLowerCase(), (String) m.get("code")))
//                                Thread.sleep(1000L);

                        //建表成功后  下线该模型任务
                        publishOneModelV2((String) m.get("id"), layer.toLowerCase(), false);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                // 任务为发布状态
                else if (m.get("status").equals("PUBLISHED")) {
                    try {
                        // 先下线任务
                        String result = publishOneModelV2((String) m.get("id"), layer.toLowerCase(), false);
                        Thread.sleep(1000L);

                        // 再上线建表
                        publishOneModelV2((String) m.get("id"), layer.toLowerCase(), true);
                        //等待建表 未建成功继续等待
                        Thread.sleep(1200L);
//                            while (!isExist(fileSystem, layer.toLowerCase(), (String) m.get("code")))
//                                Thread.sleep(1000L);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
//                }
            }
        } catch (Exception ee) {
            ee.printStackTrace();
        }
    }

    /**
     * 发布单个模型
     */
    public static String publishOneModelV2(String id, String layer, boolean isPublish) {
        String url = baseUrl + "/data-work/models/" + layer + "/" + (isPublish ? "publish" : "offline");
        String method = "POST";
        String param = "{\n" +
                " \"id\": \"" + id + "\"\n" +
                "}";
        String restr = sendByHttpCore(url, param, method, true);
        System.out.println(id + ",返回:" + restr);
        return restr;
    }

    /**
     * 发布单个模型
     */
    public static void publishOneModel(String id, String layer, boolean isPublish) {
        String url = baseUrl + "/data-work/models/" + layer + "/" + (isPublish ? "publish" : "offline");
        String method = "POST";
        String param = "{\n" +
                " \"id\": \"" + id + "\"\n" +
                "}";
        String restr = sendByHttpCore(url, param, method, true);
        System.out.println(id + ",返回:" + restr);
    }

    /**
     * 判断 表 是否存在
     */
    public static boolean isExist(org.apache.hadoop.fs.FileSystem fileSystem, String dataset, String tableName) {
        try {
            String path = "/user/hive/warehouse/" + dataset + ".db/" + tableName + "/";
            Path filePath = new Path(path);
            return fileSystem.exists(filePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 查询 stg表 meta
     */
    public static List<Map> queryTableList(String lay) {
        String url = baseUrl + "/data-integration/physics/config/queryTable";
        String method = "GET";
        String param = "{}";
        String restr = sendByHttpCore(url, param, method, true);
        // System.out.println(restr);
        Map data = gson.fromJson(restr, Map.class);
        List<Map> dataList = (List<Map>) data.get("data");
        List stgTableList = dataList.stream().collect(Collectors.toList());
        return stgTableList;
    }

    /**
     * 查询 指定层表 meta
     */
    public static List<Map> queryTableListForAll(String lay) {
        List tempList = new ArrayList();
        for (int i = 1; ; i++) {
            String url = baseUrl + "/data-work/models/page?pageNo=" + i + "&pageSize=100&dataLayer=" + lay.toUpperCase() + "&updateTime=&createTime=&owner=&creator=&nameOrCode=&status=";
            String method = "GET";
            String param = "{}";
            String restr = sendByHttpCore(url, param, method, true);
            Map data = gson.fromJson(restr, Map.class);
            //System.out.println(data);
            List<Map> dataList = (List<Map>) ((Map) data.get("data")).get("rows");
            tempList.addAll(dataList);
            if (dataList.size() < 100) break;
        }
        return tempList;
    }

    /**
     * 根据表名从 datatable 获取的tableList中获取 table Id
     */
    public static String getTableId(String tableName) {
        for (Map map : tableList) {
            if (tableName.equalsIgnoreCase((String) map.get("name"))) return (String) map.get("id");
        }
        return "";
    }

    /**
     * 根据表名从 datatable 获取的tableList中获取 table Id
     */
    public static String getTableIdByTableCode(String tableCode) {
        for (Map map : tableList) {
            if (tableCode.equalsIgnoreCase((String) map.get("code"))) return (String) map.get("id");
        }
        return "";
    }

    /**
     * 创建stg层的表
     */
    public static void createTableByFile2(String filePath, String layer, String datasetId) {
        File file = new File(filePath);
        List<String> ids = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file));) {
            ids = bufferedReader.lines().collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean realease = true;

        for (String line : ids) {
            try {
                String ary[] = line.split("\t");
                String sourceTableName = ary[0];
                String table_comment = ary[4];
                String table_name = sourceTableName; // ary[3];
                String table_subject = ary[2];

                // tableCode tableCode tableName treeID
                createByDeltaTable(sourceTableName, table_name, table_comment, table_subject, layer, datasetId);
                //createByDeltaTable(sourceTableName,table_name,table_comment,table_subject,layer,datasetId);
                Thread.sleep(10000L);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建stg层的表
     */
    public static void createTableByDataTable(String layer, String datasetId) {
        tableList = queryTableListForAll(layer);
        for (Map row : tableList) {
//            if ((row.get("code")).equals("stg_deda_building_info_econ")){
            try {
                // fixme: key 取值逻辑待确认
                String sourceTableName = (String) row.get("code");
                String table_comment = (String) row.get("name");
                String table_subject = (String) row.get("treeId");

                String targetTableName = sourceTableName + "_wzw_test1";

                datasetId = (String) row.get("datasetId");

                // table code
                createByDeltaTable(sourceTableName, targetTableName, table_comment, table_subject, layer, datasetId);
                // todo: 控制建表时间间隔
                Thread.sleep(10000L);

            } catch (Exception e) {
                e.printStackTrace();
            }
//            }

        }
    }


    /**
     * 创建 stg 表
     */
    public static void createByDeltaTable(String sourceTableName, String targetTableName, String stgTableComment, String stgTableSubjectId, String dataLayer, String datasetId) {
        String url = baseUrl + "/data-integration/physics/config/createTable";
        String method = "POST";

        // 根据表名获取表字段列表
        String field = getTableFieldByDelta(sourceTableName);

        Map data = gson.fromJson(field, Map.class);
        List<Map> dataList = (List<Map>) data.get("data");

        List fieldList = dataList.stream().map(x -> {
            x.put("dataType", x.get("columnType"));
            x.put("fieldSource", "syncTask");
            return x;
        }).collect(Collectors.toList());
        String filedListParam = gson.toJson(fieldList);


        // todo: datasetId 更改
        String param = "{\n" +
                "  \"name\": \"" + stgTableComment + "\",\n" +
                "  \"code\": \"" + targetTableName + "\",\n" +
                //"  \"datasetId\": \"1579350456685379585\",\n" +    // 固定的STG层数据集的ID
                "  \"datasetId\": \"" + datasetId + "\",\n" +
                //"  \"dataLayer\": \"STG\",\n" +
                "  \"dataLayer\": \"" + dataLayer + "\",\n" +
                "  \"owner\": \"admin\",\n" +
                "  \"username\": \"admin\",\n" +
                "  \"tableType\": \"common\",\n" +
                "  \"treeId\": \"" + stgTableSubjectId + "\",\n" +      //所属主题域的ID
                "  \"description\": \"\",\n" +
                "  \"fieldList\":" + filedListParam + ",\n" +
                "  \"datatableBaseReq\": {\n" +
                "    \"isPartition\": false,\n" +
                "    \"zoneTargetFieldCode\": \"\",\n" +
                "    \"isZipperTable\": false,\n" +
                "    \"zipperOrderByFieldCodes\": []\n" +
                "  }\n" +
                "}";
        //System.out.println("create table "+targetTableName+" response:"+param);

        System.out.println("请求参数" + param);
        String restr = sendByHttpCore(url, param, method, true);
        //DoPost(url,param,"utf8",method);
        System.out.println("create table " + targetTableName + " response:" + restr);
    }


    /**
     * 获取表字段
     */
    public static String getTableFieldByDelta(String sourceTableName) {
        // 根据 table code 获取
        String tableId = getTableIdByTableCode(sourceTableName);
        if (tableId == null || "".equals(tableId)) {
            System.out.println("表名" + sourceTableName + "不存在 或查不到表ID");
            return null;
        }
        String url = baseUrl + "/data-integration/physics/config/queryField?tableId=" + tableId;
        String method = "GET";
        String param = "";
        String restr = sendByHttpCore(url, param, method, true);

        System.out.println("response(字段List):" + restr);

        return restr;
    }


    /**
     * url 请求
     */
    public static String sendByHttpCore(String url, String json, String method, boolean is_json) {

        String result = null;
        int connectionTimeout = 60000;

        CloseableHttpResponse response = null;
        CloseableHttpClient httpclient = null;
        try {
            httpclient = HttpClients.createDefault();// new SSLClient();
            if ("POST".equalsIgnoreCase(method)) {
                HttpPost httpPost = new HttpPost(String.valueOf(url));

                //MultipartEntityBuilder mEntityBuileder = MultipartEntityBuilder.create();
                //mEntityBuileder.setCharset(Charset.forName("UTF-8"));
                // mEntityBuileder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
                //以下两个 file msg为固定字段
                //mEntityBuileder.addBinaryBody("file", file, ContentType.DEFAULT_BINARY, fileName);
                //mEntityBuileder.addTextBody("msg", msg);
                //httpPost.setEntity(mEntityBuileder.build());
                StringEntity entityParams = new StringEntity(json, "utf-8");
                httpPost.setEntity(entityParams);

                httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");//application/json
                if (is_json) httpPost.setHeader("Content-Type", "application/json");

                httpPost.setHeader("projectid", "-1");
                httpPost.setHeader("Programid", "1");
                httpPost.setHeader("Spaceid", "1");
                if (Authorization != null && !"".equals(Authorization))
                    httpPost.setHeader("Authorization", Authorization);
                if (cookie != null && !"".equals(cookie)) httpPost.setHeader("cookie", cookie);

                // 设置请求和连接超时时间
                httpPost.setConfig(RequestConfig.custom().setSocketTimeout(connectionTimeout).setConnectTimeout(connectionTimeout).build());
                ;//.setProxy(new HttpHost("localhost", 8888)).build());

                response = httpclient.execute(httpPost);
            } else if ("PUT".equalsIgnoreCase(method)) {
                HttpPut httpPost = new HttpPut(String.valueOf(url));

                //MultipartEntityBuilder mEntityBuileder = MultipartEntityBuilder.create();
                //mEntityBuileder.setCharset(Charset.forName("UTF-8"));
                // mEntityBuileder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
                //以下两个 file msg为固定字段
                //mEntityBuileder.addBinaryBody("file", file, ContentType.DEFAULT_BINARY, fileName);
                //mEntityBuileder.addTextBody("msg", msg);
                //httpPost.setEntity(mEntityBuileder.build());
                StringEntity entityParams = new StringEntity(json, "utf-8");
                httpPost.setEntity(entityParams);
                httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8"); //application/json
                httpPost.setHeader("projectid", "-1");
                httpPost.setHeader("Programid", "1");
                httpPost.setHeader("Spaceid", "1");
                if (Authorization != null && !"".equals(Authorization))
                    httpPost.setHeader("Authorization", Authorization);
                if (cookie != null && !"".equals(cookie)) httpPost.setHeader("cookie", cookie);

                // 设置请求和连接超时时间
                httpPost.setConfig(RequestConfig.custom().setSocketTimeout(connectionTimeout).setConnectTimeout(connectionTimeout).build());
                ;//.setProxy(new HttpHost("localhost", 8888)).build());

                response = httpclient.execute(httpPost);
            } else {
                HttpGet httpGet = new HttpGet(String.valueOf(url));

                httpGet.setHeader("Content-Type", "application/json");
                httpGet.setHeader("projectid", "-1");
                httpGet.setHeader("Programid", "1");
                httpGet.setHeader("Spaceid", "1");
                if (Authorization != null && !"".equals(Authorization))
                    httpGet.setHeader("Authorization", Authorization);
                if (cookie != null && !"".equals(cookie)) httpGet.setHeader("cookie", cookie);


                httpGet.setConfig(RequestConfig.custom().setSocketTimeout(connectionTimeout).setConnectTimeout(connectionTimeout).build());
                response = httpclient.execute(httpGet);
            }

            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    result = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                    EntityUtils.consume(entity);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            // 关闭连接,释放资源
            try {
                if (null != response) {
                    response.close();
                }
                if (null != httpclient) {
                    httpclient.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return result;
        }
    }

}
