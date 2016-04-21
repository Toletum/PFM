package org.toletum.pfm;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Utils {
	public static String clearNumber(String numero) {
		
		return numero.replaceAll("[^0-9.-]", "");
	}
	
	public static Integer getMonth(String f){
		Integer month;
		
		try {
			SimpleDateFormat format1=new SimpleDateFormat("MM/dd/yyyy");
			Date dt1=format1.parse(f);
			DateFormat format2=new SimpleDateFormat("MM"); 
			month=Integer.valueOf(format2.format(dt1));
		} catch(Exception ex) {
			month = new Integer(-1);
		}
		
		
		return month;
	}
	
	public static Integer getMonth2(String f){
		Integer month;
		
		try {
			SimpleDateFormat format1=new SimpleDateFormat("yyyy-MM-dd");
			Date dt1=format1.parse(f);
			DateFormat format2=new SimpleDateFormat("MM"); 
			month=Integer.valueOf(format2.format(dt1));
		} catch(Exception ex) {
			month = new Integer(-1);
		}
		
		
		return month;
	}
	
	public static Integer getDayOfWeek(String f){
		Integer dayOfWeek;
		
		try {
			SimpleDateFormat format1=new SimpleDateFormat("MM/dd/yyyy");
			Date dt1=format1.parse(f);
			DateFormat format2=new SimpleDateFormat("u"); 
			dayOfWeek=Integer.valueOf(format2.format(dt1));
		} catch(Exception ex) {
			dayOfWeek = new Integer(-1);
		}
		
		
		return dayOfWeek;
	}
	
	public static Integer getDayOfWeek2(String f){
		Integer dayOfWeek;
		
		try {
			SimpleDateFormat format1=new SimpleDateFormat("yyyy-MM-dd");
			Date dt1=format1.parse(f);
			DateFormat format2=new SimpleDateFormat("u"); 
			dayOfWeek=Integer.valueOf(format2.format(dt1));
		} catch(Exception ex) {
			dayOfWeek = new Integer(-1);
		}
		
		
		return dayOfWeek;
	}
	
	public static Integer getMinutes(String hora) {
		Integer Minutes;
		
		int h,m;
		try {
			SimpleDateFormat format1=new SimpleDateFormat("H:m:s");
			
			Date dt1=format1.parse(hora);
			DateFormat format2=new SimpleDateFormat("H"); 
			h=Integer.valueOf(format2.format(dt1));
			
			/* SOLO TENER EN CUENTA LAS HORAS
			format2=new SimpleDateFormat("m"); 
			m=Integer.valueOf(format2.format(dt1));
			*/
			m=0;
			
			Minutes = new Integer(h*60+m);
		} catch(Exception ex) {
			try {
				h=Integer.valueOf(hora.substring(0, 2));
				/* SOLO TENER EN CUENTA LAS HORAS*/
				//m=Integer.valueOf(hora.substring(2, 4));
				m=0;
				
				Minutes = new Integer(h*60+m);
			} catch(Exception ex2) {
				Minutes = new Integer(-1);
			}
		}

		return Minutes;
	}
	
	public static Integer getCodPos(String cp) {
		Integer CodPos;

		try {
			CodPos = Integer.valueOf(Utils.clearNumber(cp));
		} catch(Exception ex) {
			CodPos = new Integer(-1);
		}
		
		return CodPos;
	}
	
	public static Integer getNum(String n) {
		Integer Num;
		
		try {
			Num = Integer.valueOf(Utils.clearNumber(n));
		} catch(Exception ex) {
			Num = new Integer(-1);
		}

		return Num;
	}
	
	public static String addHour(String now) {
		SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm");
		
		Date dt;
		try {
			dt = format.parse(now);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(dt);
			
			calendar.add(Calendar.HOUR, 1);
			
			return format.format(calendar.getTime());
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}
		
	}

	public static void main(String[] args) throws Exception {
		System.out.println(Utils.addHour("2015-01-05 23:30"));
	}
}

