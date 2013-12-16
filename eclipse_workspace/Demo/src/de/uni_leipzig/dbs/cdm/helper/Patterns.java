package de.uni_leipzig.dbs.cdm.helper;

import java.util.regex.Pattern;

/**
 * @author Jakob Runge
 * @since 2013-12-16
 * */
public class Patterns {
	/**
	 * Useful to parse a line of the movies.dat file
	 * */
	public static final Pattern moviePattern = Pattern.compile("^(\\d+)::([^:]+)::([^:]+)$");
	/**
	 * Useful to parse the year from the title of a movies.dat file line
	 * */
	public static final Pattern yearPattern = Pattern.compile(".+\\((\\d{4})\\)");
	/**
	 * Useful to parse a line from one of the r*.test files
	 * */
	public static final Pattern ratingPattern = Pattern.compile("^(\\d+)::(\\d+)::([^:]+)::(\\d+)$");
}
