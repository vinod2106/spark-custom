package com.vks.java.example;

public class Book {

	private String name;
	private String author;

	public Book(String name, String author) {
		// TODO Auto-generated constructor stub
		this.name = name;
		this.author = author;
	}

	@Override
	public String toString() {
		return "Book {" + "name='" + name + '\'' + ", author='" + author + '\'' + '}';

	}

}
