package io.toxa108.blitzar.storage.database.manager.user;

import io.toxa108.blitzar.storage.NotNull;

import java.util.HashMap;
import java.util.Map;

public class UserManagerImpl implements UserManager {
    private final Map<String, User> users;

    public UserManagerImpl() {
        this.users = new HashMap<>();
    }

    @Override
    public User createUser(@NotNull final String login, @NotNull final String password) {
        return users.put(login, new UserImpl(login, password));
    }

    @Override
    public User authorize(String login, String password) throws AccessDeniedException {
        User user = users.get(login);
        if (user.password().equals(password)) {
            return user;
        }
        throw new AccessDeniedException();
    }

    @Override
    public void clear() {
        users.clear();
    }
}
