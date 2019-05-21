package com.vks.java.example;

public class Runner {

	public static void main(String[] args) {

		BookStorage storage = new BookStorage();

		for (Book b : storage.getBooks()) {
			System.out.println(b);
		}

	}
}
