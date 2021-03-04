import com.alibaba.fastjson.JSONObject;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.dataworks_public.model.v20200518.GetMetaDBTableListRequest;
import com.aliyuncs.dataworks_public.model.v20200518.GetMetaDBTableListResponse;
import com.aliyuncs.dataworks_public.model.v20200518.GetMetaTableBasicInfoRequest;
import com.aliyuncs.dataworks_public.model.v20200518.GetMetaTableBasicInfoResponse;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OpenApiTest {
    private static final Logger LOG = LoggerFactory.getLogger(OpenApiTest.class);

    public List<String> getAllTables(IAcsClient client,String appguids){
        List<String> tableGuids=new ArrayList<String>();
        for(String appguid:appguids.split(",")){
            int pageNumber=1;
            String datasourcetype=appguid.split("\\.")[0];
            while(true){
                //GetMetaDBTableList 获取表列表
                GetMetaDBTableListRequest request = new GetMetaDBTableListRequest();
                //app guid
                request.setAppGuid(appguid);
                request.setDataSourceType(datasourcetype);
                //第i页
                request.setPageNumber(pageNumber);
                //每页大小
                request.setPageSize(50);
                try {
                    GetMetaDBTableListResponse res = client.getAcsResponse(request);
                    //数据总大小
                    Long totalCount = res.getData().getTotalCount();
                    //当前页数据列表
                    List<GetMetaDBTableListResponse.Data.TableEntityListItem> tableEntityListItems = res.getData().getTableEntityList();
                    if(tableEntityListItems.isEmpty())
                        break;
                    for (GetMetaDBTableListResponse.Data.TableEntityListItem item:tableEntityListItems) {
                        String tableguid=item.getTableGuid();
                        System.out.println(tableguid);
                        tableGuids.add(tableguid);
                    }
                    pageNumber=pageNumber+1;

                } catch (Exception e){
                    LOG.info("client.getAcsResponse-----Exception");
                    System.exit(1);
                }
            }
        }

        return tableGuids;
    }

    public String getTableBasicInfo(IAcsClient client,String tableguid){
        String jsonstr="";
        try {
            GetMetaTableBasicInfoRequest request = new GetMetaTableBasicInfoRequest();
            //odps table  guid，格式odps.{projectName}.{tableName}
            request.setTableGuid(tableguid);
            //资源类型
            request.setDataSourceType(tableguid.split("\\.")[0]);
            GetMetaTableBasicInfoResponse res = client.getAcsResponse(request);
            //表数据
            GetMetaTableBasicInfoResponse.Data info = res.getData();
            jsonstr = JSONObject.toJSON(info).toString();

        }catch (Exception e){
            LOG.info("client.GetMetaTableBasicInfoRequest-----Exception");
            System.exit(1);
        }

        return jsonstr;
    }




    public static void main(String[] args){
        IClientProfile profile = DefaultProfile.getProfile("cn-shenzhen", "LTAI4G2HMD9oLtRNcqnjcmDg", "bDDcQnKRw5uovf44XWYquV6X9G5wEL");
        DefaultProfile.addEndpoint("cn-shenzhen","dataworks-public", "dataworks.cn-shenzhen.aliyuncs.com");
        IAcsClient client = new DefaultAcsClient(profile);

        OpenApiTest t=new OpenApiTest();
        List<String> res=t.getAllTables(client,"odps.dw_meta");
        System.out.println(res);
        String res2=t.getTableBasicInfo(client,"odps.dw_meta.m_task_resource");
        System.out.println(res2);
    }
}
