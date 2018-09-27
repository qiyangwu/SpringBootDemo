package framework.schedulejob.job.ordinary;

import java.util.List;

import framework.adminmodules.schedulejob.domain.ScheduleJob;
import framework.modules.exporttask.bo.ExportTaskBO;
import framework.modules.exporttask.domain.ExportTask;
import framework.modules.syslevelparam.domain.SysLevelParam;
import framework.sys.cache.GlobalCache;
import framework.sys.export.core.ExportThread;
import framework.sys.tools._Date;
/**
 * 超时导出文件删除定时任务类
 * @author xzb
 *
 */
public class TimeOutFileJob {
	private ExportTaskBO exportTaskBO;
	private static int default_clearcycle = 0;

	/**
	 * 到系统参数设置查找超时删除时间设置，并找出这些超时的文件信息，然后删除数据库记录及文件
	 * @param scheduleJob
	 */
	public void execute_trans(ScheduleJob scheduleJob) {
		SysLevelParam sysLevelParam = GlobalCache.getSysParamService().getSysLevelParam();
		default_clearcycle = sysLevelParam.getClearcycle();
		if(!isNeedDelete()) {
			return;
		}
		List<ExportTask> exportTaskList = getNeedDeleteExportTask();
		exportTaskBO.deleteBatchTask_log_trans(exportTaskList);
	}

	/**
	 * 获取需要删除的任务集合
	 * 
	 * @return 需删除的任务集合
	 */
	public List getNeedDeleteExportTask() {
		List return_list = null;
		String sql = getNeedDeleteTaskQuerySql();
		synchronized (ExportThread.class) {
			return_list = exportTaskBO.executeFind(ExportTask.class, sql);
		}
		return return_list;
	}

	/**
	 * 根据系统参数设置的文件生成超时天数判断是否需要删除文件,若为0则不作删除
	 * 
	 * @return true:需要删除 false:不需要
	 */
	private boolean isNeedDelete() {
		boolean return_flag = false;
		if (default_clearcycle <= 0) {
			return_flag = false;
		} else {
			return_flag = true;
		}
		return return_flag;
	}
	
	/**
	 * 获取需要删除的超时任务
	 * 
	 * @return
	 */
	private String getNeedDeleteTaskQuerySql() {
		String return_sql = "";
		/**
		 * 获取需要删除的日子（当前日往前推),由于设置的删除时间是凌晨1~6,如default_clearcycle为3，
		 * 当前日期为20160308的1~6时，这时候应删除20160304 ：240000前的文件 *
		 */
		String pointDateTime = _Date.todayPlusOrMinusDay(-Math.abs(default_clearcycle + 1)) + " 24:00:00";
		return_sql = "select * from texporttask where InsertTime <= '" + pointDateTime
				+ "'  and exists (select 1 from tListExportQueryInfo where ExportTaskPK = texporttask.pk  ) order by InsertTime asc";
		return return_sql;
	}

	public ExportTaskBO getExportTaskBO() {
		return exportTaskBO;
	}

	public void setExportTaskBO(ExportTaskBO exportTaskBO) {
		this.exportTaskBO = exportTaskBO;
	}
}
