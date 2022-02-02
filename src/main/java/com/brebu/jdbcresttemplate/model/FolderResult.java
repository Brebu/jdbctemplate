package com.brebu.jdbcresttemplate.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class FolderResult {
    private File folder;
    private List<Integer> integerList;
}
