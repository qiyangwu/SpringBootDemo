package framework.schedulejob.job.ordinary;

/**
 * 预警定时任务类
 * @author xzb
 */
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import framework.adminmodules.schedulejob.bo.ScheduleJobExecuteLogBO;
import framework.adminmodules.schedulejob.domain.ScheduleJob;
import framework.adminmodules.schedulejob.domain.ScheduleJobExecuteLog;
import framework.modules.lowvalueitemsmanagement.domain.ItemManage;
import framework.modules.notice.bo.NoticeBO;
import framework.modules.notice.domain.Notice;
import framework.modules.user.domain.User;
import framework.modules.warning.bo.WarningBO;
import framework.modules.warning.domain.Warning;
import framework.sys.context.applicationworker.ExceptionManager;
import framework.sys.tools._Date;
import framework.sys.tools._Math;
import org.apache.xmlbeans.impl.xb.xsdschema.Public;

public class WarningJob {
	private ScheduleJobExecuteLogBO logBO;
	private NoticeBO noticeBO;
	private WarningBO warningBO;

	/**
	 * 执行方法
	 * 
	 * @param jobValue
	 *            对应任务表中记录的jobValue
	 */
	public void execute_trans(ScheduleJob scheduleJob) {
		/** 添加执行日志记录 * */
		ScheduleJobExecuteLog executeLog = new ScheduleJobExecuteLog();
		executeLog.setJobPK(scheduleJob.getPk());
		executeLog.setStatus(1);
		try {
			Warning warning = warningBO.findById(scheduleJob.getJobValue());
			if (warning.getEnable() == 0) {
				return;
			}
			String tips = getWarningTips(warning);
			if(!tips.equals("")) {
				sendTips(warning, tips);
			}
		}
		/** 出现异常时，需要将异常信息ID也保存到执行日志记录中 * */
		catch (Exception e) {
			e.printStackTrace();
			String exceptionInfoId = ExceptionManager.getInstance().logException("WarningJob", "execute", scheduleJob.getPk(), e);
			executeLog.setStatus(0);
			executeLog.setExceptionInfoID(exceptionInfoId);
		}
		logBO.log_trans(executeLog);	
	}

	/**
	 * 获取预警提示信息
	 * 
	 * @param warning
	 * @return
	 */
	private String getWarningTips(Warning warning) {
		String strSql = "";
		String tips = "";

        //总资产数
        Number totalAssset = (Number) warningBO.executeFindUnique("select count(*) from tAssetRegist where AssetRegCheckFlag = 'SJZT_01' and AssetRegEnprCode = ?",warning.getOrgCode());
        tips = "本次共检查 " + totalAssset.toString() + " 条资产，";
        String warnRule = warning.getWarnRule();
        int warnDateRule = warnRule == null || warnRule.equals("") ? 0 : Integer.parseInt(warnRule);
        Date warnDate = _Date.plusOrMinusDay(new Date(), warnDateRule);
        SimpleDateFormat yyyygMMgddsdf = new SimpleDateFormat("yyyy-MM-dd");
        String warnDateStr = yyyygMMgddsdf.format(warnDate);
        String warnName = warning.getWarnName();
        String orgCode = warning.getOrgCode();
        String warnContent = warning.getWarnContent();
		
		if(warnName.equals("待处置资产预警")){
			strSql = "select count(*) from tAssetRegist where AssetRegMaturityDate is not null and AssetRegMaturityDate <= ? and AssetRegCheckFlag = 'SJZT_01' and AssetRegEnprCode = ? ";
            Number warnCount = (Number) warningBO.executeFindUnique(strSql, warnDateStr, orgCode);
            strSql = "select AssetRegDeptCode,count(*) from tassetregist where AssetRegMaturityDate is not null and AssetRegMaturityDate <= ? and AssetRegCheckFlag = 'SJZT_01' and AssetRegEnprCode = ? group by AssetRegDeptCode order by count(*) desc";
		    List<Object[]> list = warningBO.getEntityDAO().executeFind(strSql, warnDateStr, orgCode);
            String[] deptname = new String[2];
            Integer num = 0;
		    for (int i = 0; i < 2; i++) {
                Object[] objects =  list.get(i);
                deptname[i] = String.valueOf(objects[0]);
                num += Integer.parseInt(String.valueOf(objects[1]));
            }
            tips +="发现 "+warnCount.toString() +" 条即将到期待处置资产，其中 "+deptname[0]+" , "+deptname[0]+" 两个部门的待处置资产最多，占总待处置资产的 "+_Math.div(num,warnCount.doubleValue(),2)*100 + "%。";
		    return tips;

		}else if(warnName.equals("保修到期预警")){
			strSql = "select count(*) from tAssetRegist where AssetRegEnsureDate is not null and AssetRegEnsureDate <= ? and AssetRegCheckFlag = 'SJZT_01' and AssetRegAssetType not like '001001%' and AssetRegEnprCode = ? ";
            Number warnCount = (Number) warningBO.executeFindUnique(strSql, warnDateStr, orgCode);
            strSql = "select AssetRegDeptCode,count(*) from tassetregist where AssetRegEnsureDate is not null and AssetRegEnsureDate <= ? and AssetRegCheckFlag = 'SJZT_01' and AssetRegAssetType not like '001001%' and AssetRegEnprCode = ? group by AssetRegDeptCode order by count(*) desc";
            List<Object[]> list = warningBO.getEntityDAO().executeFind(strSql, warnDateStr, orgCode);
            String[] deptname = new String[2];
            Integer num = 0;
            for (int i = 0; i < 2; i++) {
                Object[] objects =  list.get(i);
                deptname[i] = String.valueOf(objects[0]);
                num += Integer.parseInt(String.valueOf(objects[1]));
            }
            tips +="发现 "+warnCount.toString() +" 条保修即将到期的资产，其中 "+deptname[0]+" , "+deptname[0]+" 两个部门的保修到期的资产最多，占总保修到期资产 "+_Math.div(num,warnCount.doubleValue(),2)*100 + "%。";
            return tips;
		}else if(warnName.equals("合同到期预警")){
			strSql = " select count(*) from tHouseLeaseContract where hlcRegEndDate is not null and hlcRegEndDate <= ? and hlcCheckFlag = 'SJZT_01' and hlcFirstEnprCode = ?";
			Number warnCount = (Number) warningBO.executeFindUnique(strSql, warnDateStr, orgCode);
            strSql = " select count(*) from tHouseLeaseContract where hlcRegEndDate is not null and hlcCheckFlag = 'SJZT_01' and hlcFirstEnprCode = ?";
            Number total = (Number) warningBO.executeFindUnique(strSql, orgCode);
            tips ="共检查了 "+total.toString()+ "份合同，发现 "+warnCount.toString() +" 份合同即将到期。";
            return tips;
        }else if(warnName.equals("租金到期预警")){
			strSql = " select count(*) from tHLCRentInfo where hlcRentInfoReceiveDate is not null and hlcRentInfoReceiveDate <= ? " 
				   + " and hlcRentInfoCheckFlag = 'SJZT_00' and hlcRentInfoFirstEnprCode = ?";
            Number warnCount = (Number) warningBO.executeFindUnique(strSql, warnDateStr, orgCode);
            strSql = " select count(*) from tHLCRentInfo where hlcRentInfoReceiveDate is not null and hlcRentInfoCheckFlag = 'SJZT_00' and hlcRentInfoFirstEnprCode = ?";
            Number total = (Number) warningBO.executeFindUnique(strSql, orgCode);
            tips ="共检查了 "+total.toString()+ "出租业务，发现 "+warnCount.toString() +" 条出租业务即将到期。";
            return tips;
		}else if(warnName.equals("物品上限预警")){
			strSql = "select x.* from tItemManage x left join tLowValueItems y on x.pk = y.LVIItemManagePK where x.maxStock < y.LVICount and x.maxStock > 0 and x.isWarn = 'YesNo_001' and x.OrgCode = ?";
		}else if(warnName.equals("物品下限预警")){
			strSql = "select x.* from tItemManage x left join tLowValueItems y on x.pk = y.LVIItemManagePK where x.minStock > y.LVICount and x.minStock > 0 and x.isWarn = 'YesNo_001' and x.OrgCode = ?";
		}else if(warnName.equals("办公用房超标预警")){
			Double curArea = ((Number)warningBO.executeFindUnique("select sum(curArea) from Thouseregist t where t.orgsyscode = ? and t.HouseSort=1 and t.houseFlag = 1 and t.powerKind='Fwxz_001'", orgCode)).doubleValue();
			Integer personNum = ((Number)warningBO.executeFindUnique("select sum(paouunifiedbzrs)  from tPeopleAndOrganWithDept t where t.paobzdw = ?", orgCode)).intValue();
			String orgName = warning.getOrgCodeDisplay();
			Double overValue = ((personNum!=null&&personNum>0)?(_Math.div(curArea, personNum,2)):curArea)-24;
			if(overValue>0){
				tips += warnContent.replace("[$4]", orgName).replace("[$5]", String.valueOf(overValue));
			}
			return tips;
		}else if(warnName.equals("办公设备超标预警")){
			String orgName = warning.getOrgCodeDisplay();
			Integer personNum = ((Number)warningBO.executeFindUnique("select sum(paouunifiedbzrs)  from tPeopleAndOrganWithDept t where t.paobzdw = ?", orgCode)).intValue();
		    String sqlString = "select sum(case when t.AssetRegAssetCurCount=0 then 1 else t.AssetRegAssetCurCount end) as total,sum(case when t.AssetRegIntrinsicCurValue > :price then (case when t.AssetRegAssetCurCount=0 then 1 else t.AssetRegAssetCurCount end) else 0 end) from tassetregist t where t.assetRegEnprCode = :orgCode and t.assetregassettype= :assetType and assetRegCheckFlag = 'SJZT_01'";	
		    List<String[]> tipVaList = new ArrayList<String[]>();
		    Map<String, Object> param = new HashMap<String, Object>();
		    param.put("orgCode", orgCode);
		    //台式机
		    param.put("assetType", "001002001001004");
		    param.put("price", 6000);
		    Object[] rows = (Object[])warningBO.executeFindUnique(sqlString, param);
		    Integer assetTotal = rows[0]!=null?((Number)rows[0]).intValue():0;
		    Integer numMax = Double.valueOf(_Math.mul(personNum,1.3)).intValue();
		    if(assetTotal>numMax||((Number)rows[1]).intValue()>0){
		    	tipVaList.add(new String[]{orgName,"[2010104]台式机",String.valueOf(rows[1]),String.valueOf(assetTotal-numMax>0?assetTotal-numMax:0)});
		    }
		    //便携计算机
		    param.put("assetType", "001002001001005");
		    param.put("price", 6000);
		    rows = (Object[])warningBO.executeFindUnique(sqlString, param);
		    assetTotal = rows[0]!=null?((Number)rows[0]).intValue():0;
		    numMax = Double.valueOf(_Math.mul(personNum,0.6)).intValue();
		    if(assetTotal>numMax||((Number)rows[1]).intValue()>0){
		    	tipVaList.add(new String[]{orgName,"[2010105]便携式计算机",String.valueOf(rows[1]),String.valueOf(assetTotal-numMax>0?assetTotal-numMax:0)});
		    }
		    //打印设备
		    param.put("assetType", "001002001006001");
		    param.put("price", 3500);
		    rows = (Object[])warningBO.executeFindUnique(sqlString, param);
		    assetTotal = rows[0]!=null?((Number)rows[0]).intValue():0;
		    numMax = Double.valueOf(_Math.mul(personNum,0.5)).intValue();
		    if(assetTotal>numMax||((Number)rows[1]).intValue()>0){
		    	tipVaList.add(new String[]{orgName,"[2010601]打印设备",String.valueOf(rows[1]),String.valueOf(assetTotal-numMax>0?assetTotal-numMax:0)});
		    }
		    //速印机
		    param.put("assetType", "001002002009");
		    param.put("price", 40000);
		    rows = (Object[])warningBO.executeFindUnique(sqlString, param);
		    assetTotal = rows[0]!=null?((Number)rows[0]).intValue():0;
		    numMax = 1;
		    if(assetTotal>numMax||((Number)rows[1]).intValue()>0||personNum<50){
		    	tipVaList.add(new String[]{orgName,"[2020900]速印机",String.valueOf(rows[1]),String.valueOf(personNum<50?assetTotal:assetTotal-1)});
		    }
		    
		    for(String[] tip:tipVaList){
		    	tips += tip[0]+"的"+tip[1]+"价值超标"+tip[2]+"件资产，资产数量超标"+tip[3]+"件资产，请及时调整使用情况。";
		    }
		    return tips;
		}else if(warnName.equals("闲置资产预警")){
			String orgName = warning.getOrgCodeDisplay();
			Integer total = 0;
			Integer maxvalue1 = 0;
			Integer maxvalue2 = 0;
			String maxAssetName1 = "";
			String maxAssetName2 = "";
			List queryList = warningBO.executeFind(Object.class,"select case when AssetRegAssetType like '001001%' then '土地、房屋及构筑物' when AssetRegAssetType like '001002%' then '通用设备' when AssetRegAssetType like '001003%' then '专用设备' " +
					" when AssetRegAssetType like '001004%' then '文物和陈列品' when AssetRegAssetType like '001005%' then '图书、档案' when AssetRegAssetType like '001006%' then '家具、用具、装具及动植物' else '' end,count(1)" +
					" from tassetregist t where t.assetRegEnprCode = ? and t.assetRegPurpose = 'Syfx_004' and AssetRegUseStatus in ('Syzt_001','Syzt_002','Syzt_003') and assetRegCheckFlag = 'SJZT_01' " +
					" group by case when AssetRegAssetType like '001001%' then '土地、房屋及构筑物' when AssetRegAssetType like '001002%' then '通用设备' when AssetRegAssetType like '001003%' then '专用设备' " +
					" when AssetRegAssetType like '001004%' then '文物和陈列品' when AssetRegAssetType like '001005%' then '图书、档案' when AssetRegAssetType like '001006%' then '家具、用具、装具及动植物' else '' end ", orgCode);
		    for(int i=0;i<queryList.size();i++){
		    	Object[] row = (Object[])queryList.get(i);
		    	String type = (String)row[0];
		    	int count = ((Number)row[1]).intValue();
		    	total = total+count;
		    	if(count>maxvalue1){
		    		maxAssetName2 = maxAssetName1;
		    		maxvalue2 = maxvalue1;
		    		maxAssetName1 = type;
		    		maxvalue1=count;
		    	}else if(count>maxvalue2){
		    		maxAssetName2 = type;
		    		maxvalue1 = count;
		    	}
		    }
		    if(total>0) {
                if (StringUtils.isBlank(maxAssetName2)) {
                    return tips + orgName + "的目前有" + total + "件闲置资产，其中" + maxAssetName1 + "的闲置资产最多，请及时调整其使用方向，增加资产使用效率。";
                } else {
                    return tips + orgName + "的目前有" + total + "件闲置资产，其中" + maxAssetName1 + "及" + maxAssetName2 + "这两大类的闲置资产最多，请及时调整其使用方向，增加资产使用效率。";
                }
            }
		}
		
		Number warnCount = 0;
		StringBuilder sb = new StringBuilder();//存放物品编号
		if (warnName.equals("物品上限预警") || warnName.equals("物品下限预警")) {
			List<ItemManage> itemList = warningBO.executeFind(ItemManage.class, strSql, orgCode);
			if (null != itemList && itemList.size() > 0) {
				warnCount = itemList.size();
				for (int i = 0, len = itemList.size(); i < len; i++) {
					if (i > 0) {
						sb.append(",");
					}
					sb.append(itemList.get(i).getImCode());
				}
			}
		} else {
			warnCount = (Number) warningBO.executeFindUnique(strSql, warnDateStr, orgCode);
		}
		if (warnCount.intValue() > 0) {
			tips = warnContent.replace("[$1]", String.valueOf(warnDateRule)).replace("[$2]", warnCount.toString()).replace("[$3]", sb.toString());
		}
		
		return tips;
	}


	/**
	 * 发送预警通知
	 * 
	 * @param warning
	 * @param tips
	 */
	private void sendTips(Warning warning, String tips) {
		String userAccounts = getNoticeUser(warning);
		String tipsMode = warning.getTipsMode();
		Date nowDate = new Date();
		if (userAccounts == null || userAccounts.equals("")) {
			return;
		}
		/** 系统内短信 * */
		if (tipsMode.indexOf("1") != -1) {
			Notice notice = new Notice();
			notice.setOrgCode(warning.getOrgCode());
			notice.setTitle(warning.getWarnName());
			notice.setContent(tips);
			notice.setAcceptLister("3");
			notice.setAcceptListerCode(userAccounts);
			notice.setLister("系统自动生成"); 
			SimpleDateFormat yyyygMMgddsdf = new SimpleDateFormat("yyyy-MM-dd");
			notice.setListerDate(yyyygMMgddsdf.format(nowDate));
			noticeBO.addNotice_log_trans(notice);
		}
		/** 手机短信* */
		if (tipsMode.indexOf("2") != -1) {

		}
		/** 邮件* */
		if (tipsMode.indexOf("3") != -1) {

		}

	}

	/**
	 * 获取需要通知的用户账号
	 * 
	 * @param warning
	 *            预警信息
	 * @return 以 | 分隔的用户账号，有可能返回空字符串
	 */
	private String getNoticeUser(Warning warning) {
		String userAccounts = warning.getUserAccounts();
		String groupCodes = warning.getGroupCodes();
		if (groupCodes == null || groupCodes.equals("")) {
			return userAccounts == null ? "" : userAccounts;
		}
		/** 查找当前单位的相应组的用户信息 * */
		String strSql = "select * from tuser where UserGroupCode in (:grouCodeArr) and UserOrgCode = :orgCode";
		Map<String, Object> param = new HashMap<String, Object>(2);
		String[] groupCodesArr = groupCodes.split("\\|");
		param.put("grouCodeArr", groupCodesArr);
		param.put("orgCode", warning.getOrgCode());
		List<User> userList = warningBO.executeFind(User.class, strSql, param);
		if (userList != null && userList.size() > 0) {
			for (int i = 0; i < userList.size(); i++) {
				userAccounts += "|" + userList.get(i).getUserAccount();
			}
		}
		if (userAccounts.length() > 0 && userAccounts.indexOf("|") == 0) {
			userAccounts = userAccounts.substring(1);
		}
		return userAccounts;
	}

	public WarningBO getWarningBO() {
		return warningBO;
	}

	public void setWarningBO(WarningBO warningBO) {
		this.warningBO = warningBO;
	}

	public NoticeBO getNoticeBO() {
		return noticeBO;
	}

	public void setNoticeBO(NoticeBO noticeBO) {
		this.noticeBO = noticeBO;
	}

	public ScheduleJobExecuteLogBO getLogBO() {
		return logBO;
	}

	public void setLogBO(ScheduleJobExecuteLogBO logBO) {
		this.logBO = logBO;
	}
}
