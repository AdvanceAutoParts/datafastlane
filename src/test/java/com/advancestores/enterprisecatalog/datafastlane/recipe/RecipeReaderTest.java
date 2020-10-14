package com.advancestores.enterprisecatalog.datafastlane.recipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class RecipeReaderTest {

  @Test
  void readingTest1() {
    Recipe recipe = RecipeReader.read("src/test/resources/recipe1.yaml");
    assertNotNull(recipe);
    assertEquals(1, recipe.getCount());

    Container store = recipe.getContainer("$store");
    assertNotNull(store);
    assertNotNull(store.getAttribute("books"));
    assertNotNull(store.getAttribute("authors"));

    System.out.println(recipe.debug());
  }

  @Test
  void readingTest2() {
    Recipe recipe = RecipeReader.read("src/test/resources/recipe2.yaml");
    assertNotNull(recipe);
    assertEquals(2, recipe.getCount());

    Container store = recipe.getContainer("$store");
    assertNotNull(store);
    assertNotNull(store.getAttribute("books"));
    assertNotNull(store.getAttribute("authors"));

    Container books = recipe.getContainer("books");
    assertNotNull(books);
    assertNotNull(books.getAttribute("authorId"));

    System.out.println(recipe.debug());
  }

  @Test
  void readingTest3() {
    Recipe recipe = RecipeReader.read("src/test/resources/recipe3.yaml");
    assertNotNull(recipe);
    assertEquals(3, recipe.getCount());

    Container store = recipe.getContainer("$store");
    assertNotNull(store);
    assertNotNull(store.getAttribute("books"));
    assertNotNull(store.getAttribute("authors"));

    Container books = recipe.getContainer("books");
    assertNotNull(books);
    assertNotNull(books.getAttribute("authorId"));

    Container spark = recipe.getContainer("$spark");
    assertNotNull(spark);
    assertNotNull(spark.getAttribute("session"));

    System.out.println(recipe.debug());
  }

  @Test
  void readingTest4() {
    Recipe recipe = RecipeReader.read("src/test/resources/recipe4.yaml");
    assertNotNull(recipe);
    assertEquals(2, recipe.getCount());

    Container store = recipe.getContainer("$store");
    assertNotNull(store);
    assertNotNull(store.getAttribute("books"));
    assertNotNull(store.getAttribute("authors"));

    Container books = recipe.getContainer("books");
    assertNotNull(books);
    assertNotNull(books.getAttribute("authorId"));

    System.out.println(recipe.debug());
  }

  @Test
  void readingTest5() {
    Recipe recipe =
        RecipeReader.read("src/test/resources/recipe-uuid-gen.yaml");
    assertNotNull(recipe);
    assertEquals(4, recipe.getCount());

    Container store = recipe.getContainer("$store");
    assertNotNull(store);
    assertNotNull(store.getAttribute("books"));
    assertNotNull(store.getAttribute("authors"));

    Container[] allBooks = recipe.getContainers("books");
    assertEquals(2, allBooks.length);

    // finds first one in list
    Container books = recipe.getContainer("books");
    assertNotNull(books);
    assertNotNull(books.getAttribute("authorId"));

    assertNotNull(allBooks[1].getAttribute("uuid"));

    System.out.println(recipe.debug());
  }

}
