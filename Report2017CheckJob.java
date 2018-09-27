package framework.schedulejob.job.ordinary;

import java.util.List;

import framework.adminmodules.schedulejob.bo.ScheduleJobExecuteLogBO;
import framework.adminmodules.schedulejob.domain.ScheduleJob;
import framework.adminmodules.schedulejob.domain.ScheduleJobExecuteLog;
import framework.modules.notice.bo.NoticeBO;
import framework.modules.notice.domain.Notice;
import framework.modules.organization.domain.Organization;
import framework.modules.orgparams.bo.OrgParamsBO;
import framework.modules.report.upassetreport2017.upassetreport.bo.CheckBaseTableRelation2017BO;
import framework.modules.report.upassetreport2017.upassetreport.bo.DealDataAfterErrOccurs2017BO;
import framework.modules.user.domain.User;
import framework.sys.cache.GlobalCache;
import framework.sys.cache.OrgService;
import framework.sys.context.applicationworker.ExceptionManager;
import framework.sys.tools._Date;

/**
 * 2017报表数据定时检查任务类
 * @author xzb
 *
 */
public class Report2017CheckJob {
	private NoticeBO noticeBO;
	private ScheduleJobExecuteLogBO logBO;
	private CheckBaseTableRelation2017BO checkBaseTableRelation2017BO;
	private DealDataAfterErrOccurs2017BO dealDataAfterErrOccurs2017BO;
	private OrgParamsBO orgParamsBO;

	
	public void execute_trans(ScheduleJob scheduleJob) {
		/** 添加执行日志记录 * */
		ScheduleJobExecuteLog executeLog = new ScheduleJobExecuteLog();
		executeLog.setJobPK(scheduleJob.getPk());
		executeLog.setStatus(1);

		/** 遍历每条语句，执行记录 * */
		try {
			String strSql = "select depUpReportOrgCode from tDepUpReport2017 where isCollect = 'YesNo_001'";
			List orgCodeList = noticeBO.getEntityDAO().executeFind(strSql);
			int checkCount = 0;
			int errDataOrgCount = 0;
			int exceptionOrgCount = 0;
			OrgService orgService = GlobalCache.getOrgService();
 			for (int i = 0, len = orgCodeList.size(); i < len; i++) {
				Organization organization = orgService.getVOByOrgCode((String)orgCodeList.get(i));
				String orgIsUpReport = orgParamsBO.getParam(organization.getOrgCode(), "isReportCZ");
				try {
					if (orgIsUpReport.equals("1")) {
						checkCount++;
						String tips = dealDataAfterErrOccurs2017BO.collectionTableCheck97(organization.getOrgDisplayCode());
						if (tips != null && !tips.equals("") && tips.indexOf("没有找到指定单位") == -1) {
							errDataOrgCount++;
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					exceptionOrgCount++;
				}
			}
			String desc = "检查时间：" + _Date.getSystemDateTime() + "。";
			if (errDataOrgCount > 0 || exceptionOrgCount>0) {
				desc += "2017报表数据检查发现错误数据单位，出现问题数据的单位数为：" + errDataOrgCount + "个。出现程序异常的单位数为："+exceptionOrgCount+"个。一共检查单位。" + checkCount + "个。";
			} else {
				desc += "2017报表数据检查没有发现错误数据。";
			}
			addNotice(desc);
		} catch (Exception e) {
			e.printStackTrace();
			String exceptionInfoId = ExceptionManager.getInstance().logException("YcTask", "execute_trans", "2017报表数据检查", e);
			executeLog.setStatus(0);
			executeLog.setExceptionInfoID(exceptionInfoId);
		}
		logBO.log_trans(executeLog);
	}

	/**
	 * 添加通知
	 * 
	 */
	public void addNotice(String noticeContent) {
		Notice notice = new Notice();
		notice.setOrgCode("001");
		notice.setTitle("数据定时检查结果通知");
		notice.setContent(noticeContent);
		notice.setLister("系统自动生成");
		notice.setAcceptLister("3");
		String noticeUserAccount = getNoticeUserAccount();
		notice.setAcceptListerCode(noticeUserAccount);
		notice.setListerDate(_Date.getSystemDate3());
		notice.setIsTop("0");
		noticeBO.addNotice_log_trans(notice);
	}

	/**
	 * 获取数据检查通知需呀通知的用户
	 * 一般来说：除了super用户外，角色为001（超级用户）的用户也需要知晓，例如海珠，除了super还有多个superfhb，superyegg等账号
	 * 
	 * @return 符合tnotice表中AcceptListerCode字段格式的值，如 super或super|superfhb
	 */
	private String getNoticeUserAccount() {
		String noticeUserAccount = "";
		String strSql = "select * from tuser where UserAccount = 'super' or UserGroupCode = '001'";
		List<User> userList = noticeBO.getEntityDAO().executeFind(User.class, strSql);
		if (userList != null && userList.size() > 0) {
			for (int i = 0, len = userList.size(); i < len; i++) {
				noticeUserAccount += "|" + userList.get(i).getUserAccount();
			}
		}

		if (noticeUserAccount.length() > 0) {
			noticeUserAccount = noticeUserAccount.substring(1);
		}

		return noticeUserAccount;
	}

	public CheckBaseTableRelation2017BO getCheckBaseTableRelation2017BO() {
		return checkBaseTableRelation2017BO;
	}

	public void setCheckBaseTableRelation2017BO(CheckBaseTableRelation2017BO checkBaseTableRelation2017BO) {
		this.checkBaseTableRelation2017BO = checkBaseTableRelation2017BO;
	}

	public DealDataAfterErrOccurs2017BO getDealDataAfterErrOccurs2017BO() {
		return dealDataAfterErrOccurs2017BO;
	}

	public void setDealDataAfterErrOccurs2017BO(DealDataAfterErrOccurs2017BO dealDataAfterErrOccurs2017BO) {
		this.dealDataAfterErrOccurs2017BO = dealDataAfterErrOccurs2017BO;
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
	
	public OrgParamsBO getOrgParamsBO() {
		return orgParamsBO;
	}

	public void setOrgParamsBO(OrgParamsBO orgParamsBO) {
		this.orgParamsBO = orgParamsBO;
	}
}