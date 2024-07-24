<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    exclude-result-prefixes="xs"
    xmlns:alto="http://www.loc.gov/standards/alto/ns-v4#"
    xmlns="http://www.loc.gov/standards/alto/ns-v4#"
    version="1.0">
    <xsl:output method="xml" indent="yes" />
    <xsl:template match="@* | node()" >
        <xsl:copy>
            <xsl:apply-templates select="@* | node()" />
        </xsl:copy>
    </xsl:template>
    <xsl:template match="alto:TextLine" >
        <xsl:copy>
            <xsl:apply-templates select="@*" />
            <xsl:apply-templates select="node()" />
            <String>
                <xsl:attribute name="CONTENT">
                    <xsl:for-each select="alto:String | alto:SP">
                        <xsl:choose>
                            <xsl:when test="self::alto:SP"><xsl:text> </xsl:text></xsl:when>
                            <xsl:when test="self::alto:String"><xsl:value-of select="@CONTENT" /></xsl:when>
                        </xsl:choose>
                    </xsl:for-each>
                </xsl:attribute>
            </String>
        </xsl:copy>
    </xsl:template>
    <xsl:template match="alto:TextLine/text()">
        <xsl:value-of select="normalize-space()"/>
    </xsl:template>
    <xsl:template match="@HPOS|@VPOS|@WIDTH|@HEIGHT">
        <xsl:attribute name="{name()}">
            <xsl:call-template name="normalize">
                <xsl:with-param name="text" select="."/>
            </xsl:call-template>
        </xsl:attribute>
    </xsl:template>
    <xsl:template name="normalize">
        <xsl:param name="text" select="."/>
        <xsl:param name="separator" select="' '"/>
        <xsl:choose>
            <xsl:when test="not(contains($text, $separator))">
                <xsl:value-of select="format-number($text, '#')"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="format-number(substring-before($text, $separator), '#')"/>
                <xsl:text> </xsl:text>
                <xsl:call-template name="normalize">
                    <xsl:with-param name="text" select="substring-after($text, $separator)"/>
                </xsl:call-template>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    <xsl:template match="@POINTS">
        <xsl:attribute name="POINTS">
            <xsl:call-template name="normalize">
                <xsl:with-param name="text" select="."/>
            </xsl:call-template>
        </xsl:attribute>
    </xsl:template>
    <xsl:template match="@BASELINE">
        <xsl:attribute name="BASELINE">
            <xsl:call-template name="normalize">
                <xsl:with-param name="text" select="."/>
            </xsl:call-template>
        </xsl:attribute>
    </xsl:template>
    <xsl:template match="alto:Glyph"/>
    <xsl:template match="alto:String"/>
    <xsl:template match="alto:SP"/>
</xsl:stylesheet>