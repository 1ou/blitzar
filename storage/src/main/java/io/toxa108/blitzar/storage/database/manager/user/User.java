package io.toxa108.blitzar.storage.database.manager.user;

public interface User {
    /**
     * Return user login
     * @return login
     */
    String login();

    /**
     * Return user password
     * @return password
     */
    String password();
}
