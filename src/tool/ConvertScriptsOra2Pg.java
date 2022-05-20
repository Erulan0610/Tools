package tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

public class ConvertScriptsOra2Pg {

	private final static String moduleName = "bcom.c";
	private static String modNameWithoutLayer = "";

	private static void getFileNames(String rootFolder, List<String> tblFileNames, List<String> dataFileNames,
			List<String> fncAndPrcFileNames, List<String> vwFileNames) {
		File folder = new File(rootFolder);
		for (File fileEntry : folder.listFiles()) {
			if (fileEntry.getName().contains("_tbl_")) {
				tblFileNames.add(fileEntry.getName());
			}
			if (fileEntry.getName().startsWith("8") || fileEntry.getName().startsWith("2")
					|| fileEntry.getName().contains("_d_") || fileEntry.getName().contains("_data_")) {
				dataFileNames.add(fileEntry.getName());
			}
			if (fileEntry.getName().contains("_func_") || fileEntry.getName().contains("_fnc_")
					|| fileEntry.getName().contains("_proc_") || fileEntry.getName().contains("_prc_")) {
				fncAndPrcFileNames.add(fileEntry.getName());
			}
			if (fileEntry.getName().contains("_vw_")) {
				vwFileNames.add(fileEntry.getName());
			}
		}
	}

	private static String readFromFile(String filePath) {
		StringBuilder str = null;

		try {
			String ln;
			try (BufferedReader br = new BufferedReader(
					new InputStreamReader(new FileInputStream(filePath), "UTF-8"))) {
				str = new StringBuilder();

				while ((ln = br.readLine()) != null) {
					str.append(ln.replaceAll("\\r\\n|\\r|\\n", " ")).append(" ").append("\n");
				}
			}
		} catch (Exception ex) {
			System.out.println("Error reading from file '" + filePath + "'");
		}

		return str == null ? "" : str.toString();
	}

	private static void writeToFile(String filePath, String text) throws Exception {
		try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath), "UTF-8"))) {
			bw.write(text);
			bw.flush();
			bw.close();
		} catch (IOException ex) {
			System.out.println("Error writing to file '" + filePath + "'");
			throw ex;
		}
	}

	private static String replaceFirstOccurText(String str, String toReplace, String replacement) {
		StringBuilder builder = new StringBuilder();
		int start = str.indexOf(toReplace);
		if (start >= 0) {
			builder.append(str.substring(0, start));
			builder.append(replacement);
			builder.append(str.substring(start + toReplace.length()));
		} else {
			builder.append(str);
		}

		return builder.toString();
	}

	private static String replaceLastOccurText(String str, String toReplace, String replacement) {
		StringBuilder builder = new StringBuilder();
		int start = str.lastIndexOf(toReplace);
		if (start >= 0) {
			builder.append(str.substring(0, start));
			builder.append(replacement);
			builder.append(str.substring(start + toReplace.length()));
		} else {
			builder.append(str);
		}

		return builder.toString();
	}

	private static String replace2TextBetween(String str, String replacement, String fromText, String toText) {
		String upperStr = str.toUpperCase();
		StringBuilder builder = new StringBuilder();
		int start = upperStr.indexOf(fromText);
		if (start >= 0) {
			start = start + fromText.length();
			builder.append(str.substring(0, start));
			builder.append(replacement);
			int end = upperStr.indexOf(toText);
			builder.append(str.substring(end));
		} else {
			builder.append(str);
		}

		return builder.toString();
	}

	private static String get2TextBetween(String str, String fromText, String toText) {
		String upperStr = str.toUpperCase();
		int start = upperStr.indexOf(fromText);
		if (start < 0) {
			start = 0;
		} else {
			start = start + fromText.length();
		}
		int end = upperStr.indexOf(toText);
		if (end < 0 || end < start) {
			end = str.length();
		}

		return str.substring(start, end).trim();
	}

	private static void executeReplaceTblFiles(List<String> fileNames, String rootFolder, String outputFolder) {

		if (fileNames != null && !fileNames.isEmpty()) {
			for (String runFileName : fileNames) {
				try {
					String inputFilePath = rootFolder + File.separator + runFileName;
					String pgQuery = readFromFile(inputFilePath);

					int substrIdx = runFileName.indexOf("tbl_");
					String tblName = runFileName.substring(substrIdx + 4, runFileName.length() - 4);
					tblName = tblName.toUpperCase();

					pgQuery = pgQuery.replaceAll("DECLARE BEGIN IF", "do \\$\\$ DECLARE BEGIN IF");
					pgQuery = pgQuery.replaceAll("END IF;END;~~", "END IF;END \\$\\$;~~");
					pgQuery = pgQuery.replaceAll("END IF; END; ~~", "END IF;END \\$\\$;~~");
					pgQuery = pgQuery.replaceAll("END IF; END;~~", "END IF;END \\$\\$;~~");
					pgQuery = pgQuery.replaceAll("VARCHAR2", "character varying");
					pgQuery = pgQuery.replaceAll("NUMBER", "numeric");
					pgQuery = pgQuery.replaceAll("DATETIME DATE", "DATETIME timestamp");
					pgQuery = pgQuery.replaceAll("ENABLE';", "MATCH FULL';");
					pgQuery = pgQuery.replaceAll("EXECUTE IMMEDIATE", "EXECUTE");
					pgQuery = pgQuery.replaceAll("ADD \\(CONSTRAINT", "ADD CONSTRAINT");
					pgQuery = pgQuery.replaceAll("ENABLE VALIDATE\\)", "");
					pgQuery = replaceLastOccurText(pgQuery, "PK_" + tblName, " ");
					pgQuery = replace2TextBetween(pgQuery, " ", "PRIMARY KEY", "USING");
					pgQuery = pgQuery.trim().replaceAll("[ ]{2,}", " ");

					String outputFilePath = outputFolder + File.separator + runFileName;
					writeToFile(outputFilePath, pgQuery);
				} catch (Exception e) {
					System.out.println("executeReplaceTblFiles() file name:" + runFileName + " ex:" + e.getCause());
				}
			}
		}
	}

	private static void executeReplaceVwFiles(List<String> fileNames, String rootFolder, String outputFolder) {

		if (fileNames != null && !fileNames.isEmpty()) {
			for (String runFileName : fileNames) {
				try {
					String inputFilePath = rootFolder + File.separator + runFileName;
					String pgQuery = readFromFile(inputFilePath);

					pgQuery = pgQuery.replaceAll("WITH READ ONLY", "");
					pgQuery = pgQuery.replaceAll("nvl", "coalesce");
					pgQuery = pgQuery.replaceAll("NVL", "coalesce");

					String outputFilePath = outputFolder + File.separator + runFileName;
					writeToFile(outputFilePath, pgQuery);
				} catch (Exception e) {
					System.out.println("executeReplaceVwFiles() file name:" + runFileName + " ex:" + e.getCause());
				}
			}
		}
	}

	private static void executeReplaceFncPrcFiles(List<String> fileNames, String rootFolder, String outputFolder) {

		if (fileNames != null && !fileNames.isEmpty()) {
			for (String runFileName : fileNames) {
				try {
					String inputFilePath = rootFolder + File.separator + runFileName;
					String pgQuery = readFromFile(inputFilePath);

					pgQuery = pgQuery.replaceAll("IS ", "LANGUAGE 'plpgsql' \n AS \\$BODY\\$ \n DECLARE");
					pgQuery = pgQuery.replaceAll("is ", "LANGUAGE 'plpgsql' \n AS \\$BODY\\$ \n DECLARE");
					pgQuery = pgQuery.replaceAll(" IS ", "LANGUAGE 'plpgsql' \n AS \\$BODY\\$ \n DECLARE");
					pgQuery = pgQuery.replaceAll(" is ", "LANGUAGE 'plpgsql' \n AS \\$BODY\\$ \n DECLARE");
//					pgQuery = pgQuery.replaceAll("AS ", "LANGUAGE 'plpgsql' \n AS \\$BODY\\$ \n DECLARE");
//					pgQuery = pgQuery.replaceAll("as ", "LANGUAGE 'plpgsql' \n AS \\$BODY\\$ \n DECLARE");
//					pgQuery = pgQuery.replaceAll(" AS ", "LANGUAGE 'plpgsql' \n AS \\$BODY\\$ \n DECLARE");
//					pgQuery = pgQuery.replaceAll(" as ", "LANGUAGE 'plpgsql' \n AS \\$BODY\\$ \n DECLARE");
					pgQuery = replaceLastOccurText(pgQuery, "END;", "END; $BODY$;");
					pgQuery = replaceFirstOccurText(pgQuery, "RETURN", "RETURNS");
					pgQuery = pgQuery.replaceAll("NVARCHAR2", "character varying");
					pgQuery = pgQuery.replaceAll("nvarchar2", "character varying");
					pgQuery = pgQuery.replaceAll("VARCHAR2", "character varying");
					pgQuery = pgQuery.replaceAll("varchar2", "character varying");
					pgQuery = pgQuery.replaceAll("NUMBER", "numeric");
					pgQuery = pgQuery.replaceAll("number", "numeric");
					pgQuery = pgQuery.replaceAll("PRAGMA AUTONOMOUS_TRANSACTION", "--PRAGMA AUTONOMOUS_TRANSACTION");
					pgQuery = pgQuery.replaceAll("pragma autonomous_transaction", "--PRAGMA AUTONOMOUS_TRANSACTION");
					pgQuery = pgQuery.replaceAll("PK_BATCH.setStartJob", "batch_start_job");
					pgQuery = pgQuery.replaceAll("pk_batch.setstartjob", "batch_start_job");
					pgQuery = pgQuery.replaceAll("PK_BATCH.SETSTARTJOB", "batch_start_job");
					pgQuery = pgQuery.replaceAll("PK_BATCH.ADDBATCHDIRECT", "batch_add_batch_direct");
					pgQuery = pgQuery.replaceAll("pk_batch.addbatchdirect", "batch_add_batch_direct");
					pgQuery = pgQuery.replaceAll("PK_COMMON.GETNEXTJRNO", "fnc_pk_common_getnextjrno");
					pgQuery = pgQuery.replaceAll("pk_common.getnextjrno", "fnc_pk_common_getnextjrno");
					pgQuery = pgQuery.replaceAll("PK_GEN.GETGENCONFIG", "fnc_pk_gen_getgenconfig");
					pgQuery = pgQuery.replaceAll("pk_gen.getgenconfig", "fnc_pk_gen_getgenconfig");
					pgQuery = pgQuery.replaceAll("PK_BATCH.CREATEJOB", "batch_create_job");
					pgQuery = pgQuery.replaceAll("pk_batch.createjob", "batch_create_job");
					pgQuery = pgQuery.replaceAll("PK_NES_EXP.WRITELOG", "call prc_pk_nes_exp_writelog");
					pgQuery = pgQuery.replaceAll("pk_nes_exp.writelog", "call prc_pk_nes_exp_writelog");
					pgQuery = pgQuery.replaceAll("PK_NES_EXP.NES_EXCEPTION", "call prc_pk_nes_exp_nes_exception");
					pgQuery = pgQuery.replaceAll("pk_nes_exp.nes_exception", "call prc_pk_nes_exp_nes_exception");
					pgQuery = pgQuery.replaceAll("PK_COMMON.getkeyrange", "fnc_pk_common_getkeyrange");
					pgQuery = pgQuery.replaceAll("pk_common.getkeyrange", "fnc_pk_common_getkeyrange");
					pgQuery = pgQuery.replaceAll("PK_COMMON.GETKEYRANGE", "fnc_pk_common_getkeyrange");
					pgQuery = pgQuery.replaceAll("DBMS_OUTPUT\\.put_line \\(", "RAISE NOTICE USING MESSAGE =");
					pgQuery = pgQuery.replaceAll("DBMS_OUTPUT\\.PUT_LINE \\(", "RAISE NOTICE USING MESSAGE =");
					pgQuery = pgQuery.replaceAll("BATCH_ATTR", "tp_pk_bat_attr");
					pgQuery = pgQuery.replaceAll("batch_attr", "tp_pk_bat_attr");
					pgQuery = pgQuery.replaceAll(" CLOB;", " text;");
					pgQuery = pgQuery.replaceAll(" BULK COLLECT ", " ");
					pgQuery = pgQuery.replaceAll(" bulk collect ", " ");
					pgQuery = pgQuery.replaceAll("RETURN CREATE OR REPLACE FUNCTION", "RETURN ");
					pgQuery = pgQuery.replaceAll("$$PLSQL_LINE", "1");
					pgQuery = pgQuery.replaceAll("$$plsql_line", "1");
					pgQuery = pgQuery.replaceAll("$$PLSQL_UNIT", "pk" + modNameWithoutLayer);
					pgQuery = pgQuery.replaceAll("$$plsql_unit", "pk" + modNameWithoutLayer);
					pgQuery = pgQuery.replaceAll("RESULT_CACHE RELIES_ON", "--RESULT_CACHE RELIES_ON");
					pgQuery = pgQuery.replaceAll("SYS_REFCURSOR", "REFCURSOR");
					pgQuery = pgQuery.replaceAll("nvl", "coalesce");
					pgQuery = pgQuery.replaceAll("NVL", "coalesce");

					String outputFilePath = outputFolder + File.separator + runFileName;
					writeToFile(outputFilePath, pgQuery);
				} catch (Exception e) {
					System.out.println("executeReplaceFncPrcFiles() file name:" + runFileName + " ex:" + e.getCause());
				}
			}
		}
	}

	private static String getPkFields(String str, String alias) {
		String keys = "";
		String srcKeys = get2TextBetween(str, " ON ", ")");
		srcKeys = srcKeys.toUpperCase();
		srcKeys = srcKeys.replace("(", "");
		srcKeys = srcKeys.replace(alias + ".", "");
		String[] arr = srcKeys.split("``|AND");

		for (int i = 0; i < arr.length; i++) {
			int idx = arr[i].indexOf("=");
			if (idx >= 0) {
				keys = keys + arr[i].substring(0, idx);

				if (i < arr.length - 1) {
					keys = keys + ",";
				}
			}
		}

		return keys;
	}

	private static String getSetFields(String upperSrc, String src, String alias) {

		int matchedIdx = upperSrc.indexOf("WHEN MATCHED");
		int notMatchedIdx = upperSrc.indexOf("WHEN NOT MATCHED");

		String res = "";
		if (matchedIdx > -1 && matchedIdx < notMatchedIdx) {
			res = get2TextBetween(src, "SET", "WHEN NOT MATCHED");
		} else {
			res = get2TextBetween(src, "SET", "~~");
		}
		res = res.replace(alias + ".", "");
		String contAlias = get2TextBetween(res, "=", ".");
		if (contAlias != null && contAlias.length() < 4) {
			res = res.replaceAll(contAlias + "\\.", "EXCLUDED.");
		}

		return res;
	}

	private static void convertDataFiles(List<String> fileNames, String rootFolder, String outputFolder) {
		if (fileNames != null && !fileNames.isEmpty()) {
			StringBuilder allPgQuery;
			for (String runFileName : fileNames) {
				int file_idx = 0;
				try {
					String inputFilePath = rootFolder + File.separator + runFileName;
					String srcQuery = readFromFile(inputFilePath);

					allPgQuery = new StringBuilder();

					String[] queries = srcQuery.split("``|~~");

					for (int j = 0; j < queries.length; j++) {
						file_idx++;
						String q = queries[j];
						q = q.trim();
						q = q.replaceAll("\t", " ");
						q = q.replaceAll(String.valueOf((char) 65279), " ");
						q = q.trim().replaceAll("[ ]{2,}", " ");

						if (!q.isEmpty()) {
							String qUpper = q.toUpperCase();
							if (qUpper.startsWith("MERGE") || qUpper.contains("MERGE")) {
								StringBuilder pgQuery = new StringBuilder();
								pgQuery.append("INSERT INTO ");

								// get table name with alias
								String tmp = get2TextBetween(q, "MERGE INTO", "USING");
								String[] tmpArray = tmp.split("``| ");
								String tableName = tmpArray[0];
								String alias = "";
								if (tmpArray.length > 1) {
									alias = tmpArray[1].toUpperCase();
								}
								pgQuery.append(tableName);

								// get columns
								tmp = get2TextBetween(q, "INSERT", "VALUES");
								pgQuery.append(tmp);
								pgQuery.append(" SELECT ");

								// get values
								tmp = get2TextBetween(q, "USING", " ON ");
								if ("DUAL".equals(tmp.toUpperCase())) {
									tmp = get2TextBetween(q, "VALUES", "WHEN MATCHED");
									pgQuery.append(tmp.substring(1, tmp.length() - 1));
								} else {
									tmp = get2TextBetween(q, "SELECT", " FROM ");
									pgQuery.append(tmp);
								}
								if (qUpper.contains("FROM GEN_COMPANY")) {
									pgQuery.append(" FROM GEN_COMPANY ");
								}

								pgQuery.append(" ON CONFLICT (");
								pgQuery.append(getPkFields(q, alias));
								pgQuery.append(" )");

								// get update segment
								if (qUpper.contains("UPDATE ")) {
									pgQuery.append(" DO UPDATE SET ");
									pgQuery.append(getSetFields(qUpper, q, alias));
								} else {
									pgQuery.append(" DO NOTHING");
								}

								pgQuery.append("~~");

								allPgQuery.append(pgQuery);
								allPgQuery.append("\n");
							} else if (qUpper.startsWith("DECLARE") || qUpper.contains("DECLARE")) {
								String replacedSql = q;
								replacedSql = replacedSql.replaceAll("DECLARE", "do \\$\\$ DECLARE");
								replacedSql = replacedSql.replaceAll("END;", "END \\$\\$;");
								replacedSql = replacedSql.replaceAll("VARCHAR2", "character varying");
								replacedSql = replacedSql.replaceAll("NUMBER", "numeric");
								replacedSql = replacedSql.replaceAll("DATETIME DATE", "DATETIME timestamp");
								replacedSql = replacedSql.replaceAll("ENABLE';", "MATCH FULL';");
								replacedSql = replacedSql.replaceAll("EXECUTE IMMEDIATE", "EXECUTE");
								replacedSql = replacedSql.replaceAll("ADD \\(CONSTRAINT", "ADD CONSTRAINT");
								replacedSql = replacedSql.replaceAll("ENABLE VALIDATE\\)", "");
								if (replacedSql.contains("PK_GEN.MERGE_GEN_CONFIG")) {
									replacedSql = replacedSql.replaceAll("PK_GEN.MERGE_GEN_CONFIG",
											"call prc_pk_gen_merge_gen_config");
								} else {
									replacedSql = replacedSql.replaceAll("pk_gen.merge_gen_config",
											"call prc_pk_gen_merge_gen_config");
								}
								if (replacedSql.contains("PK_EOD.ADD_EOD_STEP")) {
									replacedSql = replacedSql.replaceAll("PK_EOD.ADD_EOD_STEP",
											"call prc_pk_eod_add_eod_step");
								} else {
									replacedSql = replacedSql.replaceAll("pk_eod.add_eod_step",
											"call prc_pk_eod_add_eod_step");
								}
								replacedSql = replacedSql + "~~";
								replacedSql = replacedSql.replaceAll(System.lineSeparator(), " ");
								replacedSql = replacedSql.replaceAll("\n", " ");
								replacedSql = replacedSql.replaceAll("MODIFY", "ALTER COLUMN");
								replacedSql = replacedSql.replaceAll("modify", "ALTER COLUMN");
								replacedSql = replacedSql.replaceAll("nvl", "coalesce");
								replacedSql = replacedSql.replaceAll("NVL", "coalesce");

								allPgQuery.append(replacedSql);
								allPgQuery.append("\n");
							} else {
								q = q + "~~";
								allPgQuery.append(q);
								allPgQuery.append("\n");
							}
						}
					}

					String sql = allPgQuery.toString();
					sql = sql.trim().replaceAll("[ ]{2,}", " ");
//					sql = sql.replaceAll("CREATED_EXCLUDEDY", "CREATED_BY");
//					sql = sql.replaceAll("MODIFIED_EXCLUDEDY", "MODIFIED_BY");
					sql = sql.replaceAll("SYSDATE", "now()");
					sql = sql.replaceAll("sysdate", "now()");
					String outputFilePath = outputFolder + File.separator + runFileName;
					writeToFile(outputFilePath, sql);

				} catch (Exception e) {
					System.out.println("convertDataFiles() file name:" + runFileName + " scriptno:" + file_idx + " ex:"
							+ e.getCause());
				}
			}
		}

	}

	private static void extractPackageBody(String rootFolder) throws Exception {
		List<String> pkgFileNames = new ArrayList<>();
		StringBuilder startupFncs = new StringBuilder();
		StringBuilder startupProcs = new StringBuilder();
		boolean isFunc = true;

		File folder = new File(rootFolder);
		for (File fileEntry : folder.listFiles()) {
			if (fileEntry.getName().contains("_pkb_")) {
				pkgFileNames.add(fileEntry.getName());
			}
		}

		if (pkgFileNames != null && !pkgFileNames.isEmpty()) {
			for (String runFileName : pkgFileNames) {
				try {
					String inputFilePath = rootFolder + File.separator + runFileName;
					String srcQuery = readFromFile(inputFilePath);

					String pkgName = get2TextBetween(runFileName, "_PKB_", ".SQL");
					if (!pkgName.contains("pk_")) {
						pkgName = "pk_" + pkgName;
					}

					String[] queries = srcQuery.split("FUNCTION|PROCEDURE");

					List<String> outputs = new ArrayList<>();

					String prcOrFncName;
					String newPrcOrFncName;
					String tmpStartup;
					for (int j = 0; j < queries.length; j++) {

						String q = queries[j];

						if (!q.contains("PACKAGE")) {
							String outFileName = get2TextBetween(runFileName, "*", "PKB_");

							if (q.contains("RETURN") && !q.contains("RETURN;")) {
								isFunc = true;

								prcOrFncName = get2TextBetween(q, " ", "(");
								newPrcOrFncName = "fnc_" + pkgName + "_" + prcOrFncName;
								newPrcOrFncName = newPrcOrFncName.toLowerCase();

								q = q.replace(prcOrFncName, "CREATE OR REPLACE FUNCTION " + newPrcOrFncName);

								outFileName = outFileName + newPrcOrFncName;
								outFileName = outFileName.toLowerCase();

								tmpStartup = outFileName + "$40220314153800";
								q = "--" + tmpStartup + "\n" + q;
							} else {
								isFunc = false;

								prcOrFncName = get2TextBetween(q, " ", "(");
								newPrcOrFncName = "prc_" + pkgName + "_" + prcOrFncName;
								newPrcOrFncName = newPrcOrFncName.toLowerCase();

								q = q.replace(prcOrFncName, "CREATE OR REPLACE PROCEDURE " + newPrcOrFncName);

								outFileName = outFileName + newPrcOrFncName;
								outFileName = outFileName.toLowerCase();

								tmpStartup = outFileName + "$60220314153900";
								q = "--" + tmpStartup + "\n" + q;
							}

							if (!outputs.contains(outFileName)) {
								outputs.add(outFileName);

								if (isFunc) {
									startupFncs.append('"');
									startupFncs.append(tmpStartup);
									startupFncs.append('"');
									startupFncs.append(",\n");
								} else {
									startupProcs.append('"');
									startupProcs.append(tmpStartup);
									startupProcs.append('"');
									startupProcs.append(",\n");
								}
							} else {
								outFileName = outFileName + "_1";

								if (!outputs.contains(outFileName)) {
									outputs.add(outFileName);
								} else {
									outFileName = outFileName + "_2";
									if (!outputs.contains(outFileName)) {
										outputs.add(outFileName);
									} else {
										outFileName = outFileName + "_3";
										outputs.add(outFileName);
									}
								}
							}
							String outputFilePath = rootFolder + File.separator + outFileName + ".sql";
							writeToFile(outputFilePath, q);
						}
					}
				} catch (Exception e) {
					System.out.println("extractPackageBody() file name:" + runFileName + " ex:" + e.getCause());
				}
			}

			StringBuilder startup = new StringBuilder();
			startup.append(startupFncs);
			startup.append(startupProcs);
			String outputFilePath = rootFolder + File.separator + "startup" + ".txt";
			writeToFile(outputFilePath, startup.toString());
		}
	}

	public static void main(String[] args) throws Exception {

		modNameWithoutLayer = get2TextBetween(moduleName, "\\#", ".");

//		String rootFolder = "D:/Products/nes" + File.separator + moduleName + File.separator + "db/oracle";
		String rootFolder = "D:/Products" + File.separator + "ora2pg_tmp";
		String outputFolder = "D:/Products/nes" + File.separator + moduleName + File.separator + "db/postgre";

		extractPackageBody(rootFolder);

		List<String> tblFileNames = new ArrayList<>();
		List<String> dataFileNames = new ArrayList<>();
		List<String> fncAndPrcFileNames = new ArrayList<>();
		List<String> vwFileNames = new ArrayList<>();
		getFileNames(rootFolder, tblFileNames, dataFileNames, fncAndPrcFileNames, vwFileNames);

		System.out.println("table file count:" + tblFileNames.size() + ", data file count:" + dataFileNames.size()
				+ ", fnc prc file count:" + fncAndPrcFileNames.size() + " view file count:" + vwFileNames.size());

		executeReplaceTblFiles(tblFileNames, rootFolder, outputFolder);
		convertDataFiles(dataFileNames, rootFolder, outputFolder);
		executeReplaceFncPrcFiles(fncAndPrcFileNames, rootFolder, outputFolder);
		executeReplaceVwFiles(vwFileNames, rootFolder, outputFolder);

		System.out.println("DONE :)))))))");
	}

}
