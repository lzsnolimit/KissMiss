package kimiss;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Connection;
import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class kimissRequest extends Thread {
	private static int currentPosition = 80227;
	private static int endPosition = 600000;
	private final static String baseStr = "http://product.kimiss.com/product/";
	private static Vector<String> errors = new Vector<String>();
	private static Vector<Integer> errorsId = new Vector<Integer>();
	private static boolean handleError = false;

	public kimissRequest() {
		
	}

	public static void switchError() {
		handleError = true;
		currentPosition = errors.size();
	}

	public static void read() {
		HTable table;
		Configuration configuration = HBaseConfiguration.create();
		Scan scan = new Scan();
		try {
			table = new HTable(configuration, "kismiss");
			QualifierFilter idFilter = new QualifierFilter(
					CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(
							Bytes.toBytes("id")) // 表中存在以BELONG打头的列BELONG_SITE，过滤结果为所有行的该列数据
			);
			scan.setFilter(idFilter);
			ResultScanner results = table.getScanner(scan);

			for (Result result : results) {
				for (Cell cell : result.rawCells()) {
					errors.add(new String(CellUtil.cloneValue(cell)));
					// System.out.println(new String(cell.getQualifier()));
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(errors.size());
	}

	public void run() {
		boolean status = requestFile();
		while (status) {
			status = requestFile();
		}
	}

	public static boolean requestFile() {
		String url;
		int id;
		synchronized (kimissRequest.class) {
			if (handleError) {
				if (currentPosition < 0) {
					return false;
				}
				id = currentPosition;
				currentPosition--;
			} else {
				if (currentPosition > endPosition) {
					return false;
				}
				id = currentPosition;
				currentPosition++;
			}

		}
		url = baseStr + id + "/";
		// System.out.println(url);
		Connection con = Jsoup.connect(url);// 获取连接
		con.header("User-Agent",
				"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");// 配置模拟浏览器
		Response rs;
		try {
			rs = con.execute();
			if (rs.statusCode() != 200) {
				errors.add(url);
			} else {
				Document domTree = Jsoup.parse(rs.body());// 转换为Dom树

				if (domTree.getElementById("mainbg") != null) {
					return true;
				}

				Elements mainContent = domTree.getElementsByClass("c");
				if (mainContent.size() > 0) {
					// System.out.println(mainContent.get(0).toString());

					String comment = "";
					int commentPosition = 1;
					int limit = getCommentLength(url + commentPosition);
					while (commentPosition <= limit) {
						comment += ParseComment(url + commentPosition);
						commentPosition++;
					}

					try {

						Map<String, String> paras = new HashMap<String, String>();
						paras.put("Doms", mainContent.get(0).toString());
						paras.put("Id", String.valueOf(id));
						paras.put("Map",
								domTree.getElementsByClass("detail_map").get(0)
										.toString());
						paras.put("Name",
								domTree.getElementsByClass("lib_d_t_l").get(0)
										.toString());
						paras.put("Comments", comment);
						Hbase.addData(
								mainContent.get(0)
										.getElementById("product_name").text(),
								paras);
					} catch (Exception e) {
						System.out.println(url);
					}

				}
			}

		} catch (IOException e) {
			// System.out.println(id);
			errors.add(url);
			// e.printStackTrace();
		}// 获取响应

		return true;
	}

	public static int getCommentLength(String url) {
		System.out.println(url);
		Connection con = Jsoup.connect(url);// 获取连接
		con.header("User-Agent",
				"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");// 配置模拟浏览器
		Response rs;
		try {
			rs = con.execute();
			if (rs.statusCode() != 200) {
				return 0;
			} else {
				Document docTree = Jsoup.parse(rs.body());
				Elements comments = docTree.getElementsByClass("f_remark_list");
				if (comments.get(0).children().size() != 0) {
					Element doc = docTree.getElementsByClass("c1_8_Page")
							.get(0).child(0);
					if (doc.children().size() == 1) {
						return 1;
					} else {
						return Integer.parseInt(doc.child(
								doc.children().size() - 2).text());
					}
				} else {
					return 0;
				}

			}
		} catch (IOException e) {
			return 0;
			// e.printStackTrace();
		}// 获取响应
	}

	public static String ParseComment(String url) {
		// System.out.println(url);
		Connection con = Jsoup.connect(url);// 获取连接
		con.header("User-Agent",
				"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");// 配置模拟浏览器
		Response rs;
		try {
			rs = con.execute();
			if (rs.statusCode() != 200) {
				return "";
			} else {
				Elements comments = Jsoup.parse(rs.body()).getElementsByClass(
						"f_remark_list");
				if (comments.get(0).children().size() != 0) {
					return comments.get(0).children().toString();
				} else {
					return "";
				}

			}
		} catch (IOException e) {
			return "";
		}// 获取响应
	}

	public static void writeErrors() {
		File writename = new File("Kimiss" + "Errors.txt");
		if (!writename.exists()) {
			try {
				writename.createNewFile();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(writename,
					true));
			for (int i = 0; i < errorsId.size(); i++) {
				out.write(errorsId.get(i) + "\r\n");
			}

			out.flush(); // 把缓存区内容压入文件
			out.close(); // 最后记得关闭文件
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
