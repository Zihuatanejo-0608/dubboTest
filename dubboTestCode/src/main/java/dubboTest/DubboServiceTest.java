package dubboTest;

import com.qq.aipaas.api.IDataInfoService;
import com.qq.aipaas.model.*;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by andy on 2020/4/7.
 *
 *
 *
 */
public class DubboServiceTest {
    ApplicationContext applicationContext = new ClassPathXmlApplicationContext("classpath:consumer.xml");
    XXInfoService iDataInfoService = (XXInfoService) applicationContext.getBean("XXInfoService");

    @Test
    //判断表是否存在,如果存在再判断是否是空表
    public void emptyTableTest(){
        try{
            DbInfoDTO dbInfoDTO = DbInfoDTO.builder().dbname("ods_wgcg_bdpms").flag(true).table("zengliangbiao").build();

            ResultBean<Boolean> resultBean = iDataInfoService.emptyTable(dbInfoDTO);
            System.out.println("是否请求成功:" + resultBean.getFlag());
            System.out.println("错误码:" + resultBean.getErrorCode());
            System.out.println("信息:" + resultBean.getErrorMsg());
            System.out.println("数据:" + resultBean.getData());

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void getTableListTest(){
        try{
            TableInfoDTO tableInfoDTO = new TableInfoDTO();
            tableInfoDTO.setDbName("default");//ods_wgcg_bdpms

            ResultBean<TableInfoResp> respResultBean = iDataInfoService.getTableList(tableInfoDTO);
            System.out.println("是否请求成功:" + respResultBean.getFlag());
            System.out.println("错误码:" + respResultBean.getErrorCode());
            System.out.println("错误信息:" + respResultBean.getErrorMsg());
            if (respResultBean.getData() != null){
                List<String> list = respResultBean.getData().getTables();
                for(String table : list){
                    System.out.println(table + "\n************");
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
